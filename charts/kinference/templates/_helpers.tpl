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
`app.kubernetes.io/name: kinference` is what the PodMonitor selects on.
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

{{/*
The rendered kinference config, as JSON: Helm reads every number in
values.yaml as a float64, and `toJson` still writes `1000` as `1000`.
*/}}
{{- define "kinference.config" -}}
{{- $cfg := deepCopy (.Values.config | default dict) -}}
{{- $_ := set $cfg "listen" (printf "0.0.0.0:%d" (int .Values.service.grpcPort)) -}}
{{- $_ := set $cfg "admin_listen" (printf "0.0.0.0:%d" (int .Values.service.adminPort)) -}}
{{- /* Maps keyed by name become the lists kinference reads, so one entry (or
one field of it) can be overridden without restating the rest. A null entry
drops it. */ -}}
{{- range $from, $to := dict "profiles" "profile" "workersets" "workerset" -}}
{{- $items := list -}}
{{- range $name, $item := (get $cfg $from | default dict) -}}
{{- if $item -}}
{{- $items = append $items (set $item "name" $name) -}}
{{- end -}}
{{- end -}}
{{- $_ := unset $cfg $from -}}
{{- if $items -}}
{{- $_ := set $cfg $to $items -}}
{{- end -}}
{{- end -}}
{{- /* Namespaces: unset means the release namespace; "" means all namespaces
(kinference reads an absent key that way). */ -}}
{{- range $section := list "policy" "kubernetes" -}}
{{- $s := get $cfg $section -}}
{{- if kindIs "map" $s -}}
{{- if not (hasKey $s "namespace") -}}
{{- $_ := set $s "namespace" $.Release.Namespace -}}
{{- else if empty (get $s "namespace") -}}
{{- $_ := unset $s "namespace" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $policy := get $cfg "policy" -}}
{{- if kindIs "map" $policy -}}
{{- $_ := set $policy "backend_binding_extension" .Values.extension.name -}}
{{- end -}}
{{- if or .Values.tokenizers.existingClaim .Values.tokenizers.configMap -}}
{{- $tokenize := get $cfg "tokenize" | default dict -}}
{{- $_ := set $cfg "tokenize" $tokenize -}}
{{- $local := get $tokenize "local" | default dict -}}
{{- $_ := set $tokenize "local" $local -}}
{{- if not (hasKey $local "mount_dir") -}}
{{- $_ := set $local "mount_dir" "/etc/tokenizers" -}}
{{- end -}}
{{- end -}}
{{- toPrettyJson $cfg -}}
{{- end -}}

{{/* Whether CRD-driven routing policy is on, which gates its RBAC. */}}
{{- define "kinference.policyEnabled" -}}
{{- if dig "policy" "enabled" false (.Values.config | default dict) -}}
true
{{- end -}}
{{- end -}}
