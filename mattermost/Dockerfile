# Mattermost Team Edition for arm64. Mattermost publishes container images
# for amd64 only, so this builds one from the official linux-arm64 release
# (docs/design.md §9.1). Configuration comes from MM_* environment
# variables in compose.yml.
FROM debian:trixie-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl media-types tzdata \
 && rm -rf /var/lib/apt/lists/*

# Checked against the release's published SHA-256.
ADD --checksum=sha256:7bf88344cad68f53ee01c60942ad7772f69731beeea97387e43a1384d6d36a02 \
    https://releases.mattermost.com/11.11.2/mattermost-team-11.11.2-linux-arm64.tar.gz \
    /tmp/mattermost.tar.gz

RUN tar -xzf /tmp/mattermost.tar.gz -C / \
 && rm /tmp/mattermost.tar.gz \
 && useradd --system --uid 2000 --home-dir /mattermost mattermost \
 && mkdir -p /mattermost/data /mattermost/plugins /mattermost/client/plugins \
 && chown -R mattermost:mattermost /mattermost

USER mattermost
WORKDIR /mattermost
ENV PATH=/mattermost/bin:$PATH
EXPOSE 8065
CMD ["mattermost"]
