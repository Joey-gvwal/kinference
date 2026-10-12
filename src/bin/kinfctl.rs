//! `kinfctl`, the kinference command-line tool. Everything lives in the
//! library; see [`kinference::cli::main`].

fn main() -> std::process::ExitCode {
    kinference::cli::main()
}
