NAME   := datastax/zdm-proxy
TAG    := $$(git log -1 --pretty=%H)
IMG    := ${NAME}:${TAG}

VERSION_SCRIPT := ./scripts/version.sh
LDFLAGS := $$($(VERSION_SCRIPT) ldflags)
BINARY_NAME := $$($(VERSION_SCRIPT) binary-name)

GO_BUILD_FLAGS := -ldflags "$(LDFLAGS)"

build-binary:
	@echo "Building binary: $(BINARY_NAME)"
	@go build $(GO_BUILD_FLAGS) -o $(BINARY_NAME) ./proxy

build-binary-cross:
	@echo "Building binary: $(BINARY_NAME)"
	@GOOS=$(GOOS) GOARCH=$(GOARCH) go build $(GO_BUILD_FLAGS) -o $(BINARY_NAME) ./proxy

build:
	@docker build -t ${IMG} .

push:
	@docker push ${IMG}
	@echo "Pushed to docker hub: ${IMG}"

get_current_tag:
	@echo ${IMG}

version-info:
	@$(VERSION_SCRIPT) vars

clean:
	@rm -f zdm-proxy-v*

login:
	@docker login -u ${DOCKER_USER} -p ${DOCKER_PASS}

.PHONY: build-binary build-binary-cross build push get_current_tag version-info clean login
