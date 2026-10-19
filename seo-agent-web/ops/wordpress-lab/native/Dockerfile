FROM node:24.22.0-bookworm-slim AS node
FROM php:8.3-cli-bookworm

COPY --from=node /usr/local/bin/node /usr/local/bin/node
RUN apt-get update && apt-get install -y --no-install-recommends unzip libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Download public, pinned assets at build time, never during service startup.
RUN curl --fail --location --retry 2 --max-time 180 https://wordpress.org/wordpress-6.9.10.tar.gz -o /tmp/wordpress.tar.gz \
    && echo 'a1c0dc12e2271c6afaabf329d5af7a976f8d2c8c592773178a5baaeafc2d4f4a  /tmp/wordpress.tar.gz' | sha256sum -c - \
    && tar -xzf /tmp/wordpress.tar.gz -C / \
    && curl --fail --location --retry 2 --max-time 180 https://downloads.wordpress.org/plugin/wordpress-seo.28.7.zip -o /tmp/yoast.zip \
    && echo '148b0ae90d90e82fb871bb8524755331d92154d4e30978d752f078416275420e  /tmp/yoast.zip' | sha256sum -c - \
    && unzip -q /tmp/yoast.zip -d /wordpress/wp-content/plugins \
    && curl --fail --location --retry 2 --max-time 180 https://downloads.wordpress.org/plugin/sqlite-database-integration.2.2.23.zip -o /tmp/sqlite.zip \
    && echo '44be096a14ebcea424b5e4bf764436ec85fb067f74ab47822c4c5346df21591e  /tmp/sqlite.zip' | sha256sum -c - \
    && unzip -q /tmp/sqlite.zip -d /wordpress/wp-content/plugins \
    && cp /wordpress/wp-content/plugins/sqlite-database-integration/db.copy /wordpress/wp-content/db.php \
    && mkdir -p /wordpress/wp-content/mu-plugins \
    && rm /tmp/wordpress.tar.gz /tmp/yoast.zip /tmp/sqlite.zip

COPY hosted-proxy.mjs hosted-seed.php /opt/noyaru/
COPY native/config.php native/install.php native/router.php native/no-mail.php native/run.mjs /opt/noyaru/native/
RUN cp /opt/noyaru/native/config.php /wordpress/wp-config.php \
    && cp /opt/noyaru/native/no-mail.php /wordpress/wp-content/mu-plugins/noyaru-demo.php \
    && php -r 'exit(extension_loaded("pdo_sqlite") && extension_loaded("sqlite3") ? 0 : 1);' \
    && php -l /opt/noyaru/native/config.php && php -l /opt/noyaru/native/install.php \
    && php -l /opt/noyaru/native/router.php && php -l /opt/noyaru/hosted-seed.php \
    && node --check /opt/noyaru/native/run.mjs \
    && node --version

USER www-data
EXPOSE 10000
ENTRYPOINT ["node", "/opt/noyaru/native/run.mjs"]
