FROM nginx:latest

COPY default.template.conf /etc/nginx/templates/default.template.conf
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
