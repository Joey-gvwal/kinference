{{/*
Release name (or override). Used as the base name for PV/PVC/Deployment/Service.
*/}}
{{- define "vllm-server.name" -}}
{{- default .Release.Name .Values.nameOverride | trunc 53 | trimSuffix "-" -}}
{{- end -}}

{{/*
Labels stamped on every object so `kubectl get -l ...` and the monitor-config
generator can find server instances reliably. Only values that satisfy the
Kubernetes label-value regex go here.

`app.kubernetes.io/managed-by` is reserved by Helm 4 (it forces the value
to `Helm` regardless of what the chart sets). We use `mm/managed-by` as
the harness discriminator instead; `mm/engine` lets selectors filter by
inference engine; `app.kubernetes.io/component=inference` is the engine-
and model-agnostic selector for "this is an inference worker pod" (the
init-weights Job, system pods, etc. don't carry it). Note that the
component label is intentionally NOT in selectorLabels — Deployment
spec.selector is immutable after creation, and adding a new label there
would force replacement of every existing Deployment.
*/}}
{{- define "vllm-server.labels" -}}
app.kubernetes.io/name: {{ include "vllm-server.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: inference
mm/managed-by: model-harness
mm/engine: vllm
mm/server-name: {{ include "vllm-server.name" . }}
{{- end -}}

{{/*
Free-form metadata. Model names like "Qwen/Qwen2.5-7B-Instruct" contain
characters (`/`) that aren't legal in label values, so they live as
annotations. gen-monitor-config.sh reads these to assemble config.toml.
*/}}
{{- define "vllm-server.annotations" -}}
mm/model: {{ .Values.model.name | quote }}
{{- if .Values.model.blockSize }}
mm/block-size: {{ .Values.model.blockSize | quote }}
{{- end }}
{{- end -}}

{{- define "vllm-server.selectorLabels" -}}
app.kubernetes.io/name: {{ include "vllm-server.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
