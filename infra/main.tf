module "stack" {
  source = "github.com/flipbitsnotburgers/hetzner-kamal-stack//terraform?ref=master"

  hcloud_token      = var.hcloud_token
  ssh_allowed_cidrs = var.ssh_allowed_cidrs
  project_name      = "valhalla-overture"
  server_location   = "hel1"
  server_type       = "cax11"

  web_count         = 1
  accessories_count = 0
}

output "web_servers" {
  value = module.stack.web_servers
}

output "web_public_ips" {
  value = module.stack.web_public_ips
}
