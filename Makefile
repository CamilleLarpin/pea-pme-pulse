help:
	@echo "Available targets:"
	@echo "  make gcp-auth   -> GCP login + ADC"
	@echo "  make gcp-setup  -> login + ADC + projet"
	@echo "  make gcp-check  -> check gcp configuration"
	@echo "  make gcp-reset  -> reset ADC authentication and unset project"

install:

lint:

format:

clean:

.PHONY: gcp-setup gcp-auth gcp-check gcp-reset

gcp-setup: gcp-auth
	@echo "Project $(GCP_PROJECT_ID) configuration "
	gcloud config set project $(GCP_PROJECT_ID)
	@echo "Setup end"

gcp-auth:
	@if gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then \
		echo "✔ Already authentificated (gcloud)"; \
	else \
		gcloud auth login; \
	fi

	@echo "ADC verification..."

	@if gcloud auth application-default print-access-token > /dev/null 2>&1; then \
		echo "✔ ADC already configured"; \
	else \
		echo "🔐 Login ADC required"; \
		gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform; \
	fi

gcp-check:
	@echo "Active Account :"
	gcloud auth list
	@echo "\nActive project :"
	gcloud config get-value project

gcp-reset:
	@echo "Reset ADC authentication and unset project"
	gcloud auth application-default revoke
	gcloud config unset project
	@echo "Reset end"