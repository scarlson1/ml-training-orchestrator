terraform {
  required_providers {
    oci = {
      source  = "oracle/oci"
      version = "~> 6.0"
    }
  }
}

# ── Lookups ───────────────────────────────────────────────────────────────────

# Each OCI region has 1-3 availability domains. We take the first.
data "oci_identity_availability_domains" "ads" {
  compartment_id = var.compartment_id
}

# Fetch the latest Ubuntu 22.04 ARM64 image for this region.
# Images are region-specific, so we can't hardcode an image OCID.
data "oci_core_images" "ubuntu_arm" {
    compartment_id              = var.compartment_id
    operating_system            = "Canonical Ubuntu"
    operating_system_version    = "22.04"
    shape                       = var.vm_shape
    sort_by                     = "TIMECREATED"
    sort_order                  = "DESC"
    state                       = "AVAILABLE"
}

# ----- Networking ----- #

# The VCN is the top-level private network. Everything lives inside it.
resource "oci_core_vcn" "main" {
  compartment_id = var.compartment_id
  display_name   = "bmo-vcn"
  cidr_blocks    = ["10.0.0.0/16"]
  dns_label      = "bmovcn"
}

# The internet gateway gives the VCN a path to/from the public internet.
resource "oci_core_internet_gateway" "main" {
  compartment_id = var.compartment_id
  vcn_id         = oci_core_vcn.main.id
  display_name   = "bmo-igw"
  enabled        = true
}

# Route table: send all non-local traffic (0.0.0.0/0) through the IGW.
resource "oci_core_route_table" "main" {
  compartment_id = var.compartment_id
  vcn_id         = oci_core_vcn.main.id
  display_name   = "bmo-rt"

  route_rules {
    destination       = "0.0.0.0/0"
    destination_type  = "CIDR_BLOCK"
    network_entity_id = oci_core_internet_gateway.main.id
  }
}


# Security list = stateful firewall. OCI requires explicit ingress rules
# even for established return traffic (unlike AWS security groups).
resource "oci_core_security_list" "main" {
  compartment_id = var.compartment_id
  vcn_id         = oci_core_vcn.main.id
  display_name   = "bmo-sl"

  # Allow all outbound (Docker image pulls, R2 writes, etc.)
  egress_security_rules {
    destination = "0.0.0.0/0"
    protocol    = "all"
    stateless   = false
  }

  # SSH — consider restricting to your IP in production
  ingress_security_rules {
    protocol  = "6" # TCP
    source    = "0.0.0.0/0"
    stateless = false
    tcp_options {
      min = 22
      max = 22
    }
  }

  # Dagster webui
  ingress_security_rules {
    protocol  = "6"
    source    = "0.0.0.0/0"
    stateless = false
    tcp_options {
      min = 3000
      max = 3000
    }
  }

  # MLflow tracking server (called by Dagster jobs + Fly.io serving API)
  ingress_security_rules {
    protocol  = "6"
    source    = "0.0.0.0/0"
    stateless = false
    tcp_options {
      min = 5000
      max = 5000
    }
  }

  # ICMP type 3 code 4 = path MTU discovery. Required for TCP to work correctly
  # on OCI — without this, large packets get silently dropped.
  ingress_security_rules {
    protocol  = "1" # ICMP
    source    = "0.0.0.0/0"
    stateless = false
    icmp_options {
      type = 3
      code = 4
    }
  }

  # HTTP — required for Let's Encrypt ACME challenge (Caddy)
  ingress_security_rules {
    protocol  = "6"
    source    = "0.0.0.0/0"
    stateless = false
    tcp_options {
      min = 80
      max = 80
    }
  }

  # HTTPS — public inference API via Caddy TLS termination
  ingress_security_rules {
    protocol  = "6"
    source    = "0.0.0.0/0"
    stateless = false
    tcp_options {
      min = 443
      max = 443
    }
  }
}


# Public subnet. Instances here get public IPs automatically.
resource "oci_core_subnet" "main" {
  compartment_id    = var.compartment_id
  vcn_id            = oci_core_vcn.main.id
  display_name      = "bmo-subnet"
  cidr_block        = "10.0.1.0/24"
  dns_label         = "bmosubnet"
  route_table_id    = oci_core_route_table.main.id
  security_list_ids = [oci_core_security_list.main.id]
}


# ----- VM ----- #

resource "oci_core_instance" "control_plane" {
  availability_domain = data.oci_identity_availability_domains.ads.availability_domains[0].name
  compartment_id      = var.compartment_id
#   display_name        = "bmo-control-plane"
  display_name        = "${var.project_name}-control-plane"
  shape               = var.vm_shape

  shape_config {
    ocpus         = var.vm_ocpus
    memory_in_gbs = var.vm_memory_gb
  }

  create_vnic_details {
    subnet_id        = oci_core_subnet.main.id
    assign_public_ip = false # don't assign - using reserved IP
    hostname_label   = "${var.project_name}-control-plane"
  }

  source_details {
    source_type             = "image"
    source_id               = data.oci_core_images.ubuntu_arm.images[0].id
    boot_volume_size_in_gbs = var.boot_volume_gb
  }

  metadata = {
    ssh_authorized_keys = var.ssh_public_key
    user_data           = base64encode(file("${path.module}/cloud-init.sh"))
  }

  lifecycle {
    # Ignore image ID changes — Oracle rotates image OCIDs on minor Ubuntu updates.
    # Without this, `terraform plan` would always show a replacement diff.
    ignore_changes = [source_details[0].source_id]
  }
}

resource "oci_core_public_ip" "control_plane" {
  compartment_id = var.compartment_id
  lifetime       = "RESERVED"   # survives VM termination; EPHEMERAL would not
  display_name   = "bmo-control-plane-ip"

  # Attach to the instance's primary VNIC
  private_ip_id  = data.oci_core_private_ips.primary.private_ips[0].id

  lifecycle {
    # Never destroy the IP — losing it breaks all downstream references
    prevent_destroy = true
  }
}

# Look up the primary private IP of the VNIC that was created with the instance
data "oci_core_private_ips" "primary" {
  subnet_id  = oci_core_subnet.main.id
  ip_address = oci_core_instance.control_plane.private_ip
}
