# Azure infrastructure for nuthatch integration tests

variable "azure_container_name" {
  description = "Name of the test container"
  type        = string
  default     = "nuthatch-test"
}

variable "azure_location" {
  description = "Azure region"
  type        = string
  default     = "eastus"
}

# login via azure cli and set subscription
# az login
# export ARM_SUBSCRIPTION_ID=$(az account show --query id -o tsv)

provider "azurerm" {
  features {}
}

resource "random_id" "storage" {
  byte_length = 4
}

resource "azurerm_resource_group" "test" {
  name     = "nuthatch-test-rg"
  location = var.azure_location
}

resource "azurerm_storage_account" "test" {
  name                     = "nuthatchtest${random_id.storage.hex}"
  resource_group_name      = azurerm_resource_group.test.name
  location                 = azurerm_resource_group.test.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "test" {
  name                  = var.azure_container_name
  storage_account_id    = azurerm_storage_account.test.id
  container_access_type = "private"
}

output "azure_container_name" {
  value = azurerm_storage_container.test.name
}

output "azure_connection_string" {
  value     = azurerm_storage_account.test.primary_connection_string
  sensitive = true
}

output "azure_resource_group" {
  value = azurerm_resource_group.test.name
}

output "azure_storage_account" {
  value = azurerm_storage_account.test.name
}
