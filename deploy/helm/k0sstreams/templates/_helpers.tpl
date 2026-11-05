{{/*
Expand the name of the chart.
*/}}
{{- define "k0sstreams.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "k0sstreams.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "k0sstreams.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "k0sstreams.labels" -}}
helm.sh/chart: {{ include "k0sstreams.chart" . }}
{{ include "k0sstreams.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "k0sstreams.selectorLabels" -}}
app.kubernetes.io/name: {{ include "k0sstreams.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app: {{ include "k0sstreams.name" . }}
{{- end }}

{{/*
Headless service fullname
*/}}
{{- define "k0sstreams.headless.fullname" -}}
{{- include "k0sstreams.fullname" . }}
{{- end }}
