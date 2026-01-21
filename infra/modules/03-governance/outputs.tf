output "dataform_repository_name" {
  description = "Nome do repositorio Dataform (se habilitado)."
  value       = var.enable_dataform ? google_dataform_repository.iot_transformations[0].name : null
}

output "dataform_repository_id" {
  description = "ID completo do repositorio Dataform (se habilitado)."
  value       = var.enable_dataform ? google_dataform_repository.iot_transformations[0].id : null
}
