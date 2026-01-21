variable "project_id" {
  type        = string
  description = "ID do projeto GCP."
}

variable "region" {
  type        = string
  description = "Regiao para recursos do Dataform."
}

variable "enable_dataform" {
  type        = bool
  description = "Habilita Dataform (API + repo)."
  default     = true
}
