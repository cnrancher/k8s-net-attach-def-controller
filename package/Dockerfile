FROM registry.suse.com/bci/bci-base:15.6

ARG TARGETARCH
COPY dist/k8s-net-attach-def-controller_linux_${TARGETARCH}*/k8s-net-attach-def-controller /usr/bin/
ENTRYPOINT ["/usr/bin/k8s-net-attach-def-controller"]
