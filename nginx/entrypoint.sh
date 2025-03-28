#!/bin/sh

# Generate config from template
envsubst '$DOMAIN' < /etc/nginx/templates/default.template.conf > /etc/nginx/conf.d/default.conf

# Start nginx
nginx -g "daemon off;"
