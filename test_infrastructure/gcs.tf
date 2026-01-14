# GCS infrastructure for nuthatch integration tests

variable "gcs_bucket_name" {
  description = "Name of the GCS test bucket"
  type        = string
  default     = "nuthatch-test"
}

variable "gcs_project" {
  description = "GCP project ID"
  type        = string
  default     = "sheerwater"
}

variable "gcs_location" {
  description = "Bucket location"
  type        = string
  default     = "us-central1"
}

provider "google" {
  project = var.gcs_project
}

resource "google_storage_bucket" "test" {
  name                        = var.gcs_bucket_name
  location                    = var.gcs_location
  uniform_bucket_level_access = true
  force_destroy               = true
}

resource "google_service_account" "test" {
  account_id   = "nuthatch-test"
  display_name = "Nuthatch Test Service Account"
}

resource "google_storage_bucket_iam_member" "test" {
  bucket = google_storage_bucket.test.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.test.email}"
}

resource "google_service_account_key" "test" {
  service_account_id = google_service_account.test.name
}

resource "local_file" "gcs_sa_key" {
  content  = base64decode(google_service_account_key.test.private_key)
  filename = "${path.module}/nuthatch-test-sa-key.json"
}

output "gcs_bucket_name" {
  value = google_storage_bucket.test.name
}

output "gcs_key_file" {
  value = local_file.gcs_sa_key.filename
}

output "gcs_service_account_email" {
  value = google_service_account.test.email
}

output "gcs_project" {
  value = var.gcs_project
}
