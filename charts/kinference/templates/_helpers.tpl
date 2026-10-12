{{/* Chart name. */}}
{{- define "kinference.name" -}}
{{- .Chart.Name -}}
{{- end -}}

{{/*
Fully qualified name. Cluster-scoped objects (ClusterRole, ClusterRoleBinding)
prefix with this so two releases can coexist.
*/}}
{{- define "kinference.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Selector labels — the immutable Deployment selector, so keep this minimal.
`app.kubernetes.io/name: kinference` is what the PodMonitor and the raw manifests
in k8s/kinference select on.
*/}}
{{- define "kinference.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kinference.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Common labels. */}}
{{- define "kinference.labels" -}}
{{ include "kinference.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{/* Image reference. */}}
{{- define "kinference.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}
