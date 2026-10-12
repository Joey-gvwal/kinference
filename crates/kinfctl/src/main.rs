//! `kinfctl` — the kinference command-line tool.
//!
//! One command today, `check`, for catching mistakes in `InferencePolicy`
//! manifests before they are applied. Built for being driven by an assistant
//! as much as by a person: every finding names its manifest path and a stable
//! code, `-o json` is a documented schema, and the exit status alone says
//! whether there is anything to fix.
//!
//! All the judgement lives in [`kinference_policy::check`]; this crate reads
//! files and prints.

mod version;

use std::io::Read;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand, ValueEnum};

use kinference_policy::check::{self, Input, NameCheckSkip, Options, Report};
use kinference_policy::warning::WarningCode;

/// Exit statuses. Distinct for "found problems" and "could not look", so a
/// script — or an agent — never mistakes an unreadable file for a clean one.
const EXIT_CLEAN: u8 = 0;
const EXIT_FINDINGS: u8 = 1;
const EXIT_UNUSABLE: u8 = 2;

const CHECK_EXAMPLES: &str = "\
Examples:
  # Check a draft on its own (spec checks only)
  kinfctl check policy.yaml

  # Also check the names it uses against the pool it will serve. The pool
  # covers backend names; its worker sets and providers add models and labels
  kinfctl check --pool default/inference-all policy.yaml demo/*.yaml

  # Check against what is in the cluster right now (`-` is standard input).
  # -A because a destination in another namespace, and the ReferenceGrant
  # that allows it, live outside the pool's
  kubectl get infpool,iws,infprov,refgrant -A -o yaml \\
    | kinfctl check --pool default/inference-all - policy.yaml

  # Machine-readable findings
  kinfctl check -o json policy.yaml

Every finding is one line: E or W, then a manifest path
(spec.rules[0].matches[1].tenant), a stable code (UnknownBackend) and what to
do about it.
  E  wrong whatever pool serves the policy: a misspelled field, a threshold
     in the wrong units
  W  wrong for the pools it was checked against: a backend or model name
     they do not have
Either is a finding to fix. The codes are documented in the InferencePolicy
reference.

A clean policy prints only its `ok` line. Anything that limited the checks,
such as name checks skipped for want of a pool, is noted once at the top.

Exit status: 0 nothing to fix, 1 findings, 2 input that could not be checked.";

#[derive(Parser, Debug)]
#[command(
    name = "kinfctl",
    version = version::VERSION,
    about = "The kinference command-line tool",
    propagate_version = true
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Check InferencePolicy manifests for mistakes before applying them.
    ///
    /// Reports fields the API server would silently drop, values that do not
    /// parse, rules that can never be reached, thresholds outside an
    /// attribute's range, and — when the pool and its destinations are among
    /// the inputs — backend and model names that match nothing. Reads no
    /// cluster state of its own.
    #[command(after_long_help = CHECK_EXAMPLES)]
    Check(CheckArgs),
}

#[derive(Args, Debug)]
struct CheckArgs {
    /// Manifest files, each holding one or more YAML or JSON documents. `-`
    /// reads standard input. Directories are not read; use a shell glob
    /// (`dir/*.yaml`) instead.
    #[arg(required = true, value_name = "FILE")]
    files: Vec<String>,

    /// Check every policy against this pool (NAMESPACE/NAME), as if attached
    /// to it — for a draft no pool references yet. The pool and its
    /// destinations must be among the inputs. Without it, each policy is
    /// checked against the input pools whose policyRef names it.
    #[arg(long, value_name = "NAMESPACE/NAME")]
    pool: Option<String>,

    /// Namespace for objects that do not set one, as `kubectl apply -n` would.
    #[arg(short = 'n', long, default_value = "default")]
    namespace: String,

    /// Output format.
    #[arg(short = 'o', long, value_enum, default_value_t = Format::Text)]
    output: Format,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Format {
    Text,
    Json,
}

/// Parse the command line, run it, and turn the outcome into an exit status.
fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Check(args) => run_check(args),
    }
}

fn run_check(args: CheckArgs) -> ExitCode {
    let inputs = match read_inputs(&args.files) {
        Ok(i) => i,
        Err(e) => {
            eprintln!("kinfctl: {e}");
            return ExitCode::from(EXIT_UNUSABLE);
        }
    };
    let opts = Options {
        namespace: args.namespace,
        pool: args.pool,
    };
    let report = match check::check(&inputs, &opts) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("kinfctl: {e}");
            return ExitCode::from(EXIT_UNUSABLE);
        }
    };
    match args.output {
        Format::Json => println!(
            "{}",
            serde_json::to_string_pretty(&report).expect("a report always serializes")
        ),
        Format::Text => print!("{}", render_text(&report)),
    }
    ExitCode::from(if report.ok { EXIT_CLEAN } else { EXIT_FINDINGS })
}

/// Read every named input. `-` is standard input, read once.
fn read_inputs(files: &[String]) -> Result<Vec<Input>, String> {
    let mut inputs = Vec::with_capacity(files.len());
    let mut stdin_read = false;
    for f in files {
        if f == "-" {
            if stdin_read {
                return Err("`-` (standard input) given more than once".into());
            }
            stdin_read = true;
            let mut text = String::new();
            std::io::stdin()
                .read_to_string(&mut text)
                .map_err(|e| format!("<stdin>: {e}"))?;
            inputs.push(Input {
                source: "<stdin>".into(),
                text,
            });
            continue;
        }
        let path = std::path::Path::new(f);
        if path.is_dir() {
            return Err(format!(
                "{f}: is a directory. Pass files — a shell glob like `{}/*.yaml` works",
                f.trim_end_matches('/')
            ));
        }
        let text = std::fs::read_to_string(path).map_err(|e| format!("{f}: {e}"))?;
        inputs.push(Input {
            source: f.clone(),
            text,
        });
    }
    Ok(inputs)
}

/// Human-readable report. First, once, whatever limited the checks; then an
/// `ok` line per clean policy, or its findings, `E` for an error and `W` for a
/// warning; then a summary, only if there is something to fix. So a clean run
/// prints its `ok` lines and nothing else but those notes.
fn render_text(r: &Report) -> String {
    use std::collections::{BTreeMap, BTreeSet};
    use std::fmt::Write;
    let mut s = String::new();

    let skips: BTreeSet<NameCheckSkip> = r
        .policies
        .iter()
        .filter_map(|p| p.name_checks.skipped)
        .collect();
    for skip in skips {
        let _ = writeln!(
            s,
            "Note: {}",
            match skip {
                NameCheckSkip::NoPool => {
                    "Skipping name checks. Add InferencePool manifests (and if necessary, \
                     --pool) to enable."
                }
                NameCheckSkip::DestinationsNotListed => {
                    "Skipping name checks against pools that choose destinations by label or \
                     namespace. Add InferenceWorkerSet and InferenceProvider manifests to enable."
                }
                NameCheckSkip::NoDestinationManifests => {
                    "Skipping model and label checks. Add InferenceWorkerSet and \
                     InferenceProvider manifests to enable."
                }
            }
        );
    }
    let models: BTreeSet<&str> = r
        .policies
        .iter()
        .filter_map(|p| p.name_checks.models_unchecked.as_deref())
        .collect();
    for why in models {
        let _ = writeln!(s, "Note: Model names not checked: {why}");
    }
    // A destination that did not resolve is the pool's business, not the
    // policy's, unless its models or labels may be why a warning fired.
    let explains = |w: &check::Finding| {
        w.code == WarningCode::UnknownModel.as_str()
            || w.code == WarningCode::SelectorMatchesNothing.as_str()
    };
    let mut unresolved: BTreeMap<&str, BTreeSet<&check::UnresolvedDestination>> = BTreeMap::new();
    for p in r
        .policies
        .iter()
        .filter(|p| p.warnings.iter().any(explains))
    {
        for u in &p.name_checks.unresolved {
            unresolved.entry(u.pool.as_str()).or_default().insert(u);
        }
    }
    for (pool, missing) in unresolved {
        let list = missing
            .iter()
            .map(|u| format!("{} `{}` ({})", u.kind, u.name, u.reason))
            .collect::<Vec<_>>()
            .join(", ");
        // What the reasons usually mean offline: an input not passed.
        let mut why = String::new();
        if missing.iter().any(|u| u.reason == "NotFound") {
            why.push_str(". `NotFound` usually means the manifest is not among the inputs");
        }
        if missing
            .iter()
            .any(|u| u.reason == "CrossNamespaceNotGranted")
        {
            why.push_str(
                ". `CrossNamespaceNotGranted` means no ReferenceGrant among the inputs allows it",
            );
        }
        let _ = writeln!(
            s,
            "Note: Pool {pool} has destination(s) that did not resolve, so their models and \
             labels are unknown here: {list}{why}"
        );
    }
    for e in &r.other_errors {
        let _ = writeln!(
            s,
            "E {} (document {}, {}): {}: {}",
            e.source, e.document, e.kind, e.code, e.message
        );
    }

    for p in &r.policies {
        if p.is_clean() {
            let _ = writeln!(s, "{}: {}: ok", p.source, p.policy);
            continue;
        }
        let _ = writeln!(s, "{}: {}", p.source, p.policy);
        for (letter, findings) in [("E", &p.errors), ("W", &p.warnings)] {
            for f in findings {
                let _ = writeln!(s, "{letter}   {}: {}: {}", f.path, f.code, f.message);
            }
        }
    }

    if !r.ok {
        let with_findings = r.policies.iter().filter(|p| !p.is_clean()).count();
        let count = |f: fn(&check::PolicyReport) -> usize| r.policies.iter().map(f).sum::<usize>();
        let _ = writeln!(
            s,
            "\n{} polic{} checked, {} with findings: {} error(s), {} warning(s){}{}",
            r.policies.len(),
            if r.policies.len() == 1 { "y" } else { "ies" },
            with_findings,
            count(|p| p.errors.len()),
            count(|p| p.warnings.len()),
            match r.other_errors.len() {
                0 => String::new(),
                n => format!("; {n} other manifest(s) could not be read"),
            },
            match r.skipped_documents.len() {
                0 => String::new(),
                n => format!("; skipped {n} document(s) of other kinds"),
            }
        );
    }
    s
}
