FROM proxy.docker.adsrvr.org/python:3.12-slim

USER root

RUN apt-get update \
    && apt-get install -y git curl sudo python3-pip jq postgresql-client ca-certificates \
    && curl -o /usr/local/share/ca-certificates/ttd-root-ca.crt https://nexus.adsrvr.org/repository/ttd-raw/certs/ttd-root-ca.crt \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip \
    && pip3 install --no-cache-dir requests

RUN python -m pip install pip-system-certs

RUN KUBECTL_VERSION=$(curl -L -s https://dl.k8s.io/release/stable.txt) && \
    curl -LO "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" && \
    curl -LO "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl.sha256" && \
    echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check && \
    install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl && \
    rm kubectl kubectl.sha256

ENV HELM_VERSION=3.13.1

RUN case `uname -m` in \
            x86_64) ARCH=amd64; ;; \
            aarch64) ARCH=arm64; ;; \
            *) echo "un-supported arch, exit ..."; exit 1; ;; \
        esac && \
    curl -L https://get.helm.sh/helm-v${HELM_VERSION}-linux-${ARCH}.tar.gz | tar xz \
    && mv linux-${ARCH}/helm /usr/local/bin/helm \
    && rm -rf linux-${ARCH} \
    && chmod +x /usr/local/bin/helm
