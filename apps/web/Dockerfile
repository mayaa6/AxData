# Build context is the repo root: the web app imports markdown from ../../docs.
FROM node:22-alpine AS build

WORKDIR /app

COPY apps/web/package.json ./apps/web/
RUN npm install --prefix apps/web

COPY docs ./docs
COPY apps/web ./apps/web
RUN npm run build --prefix apps/web

FROM nginx:alpine
COPY --from=build /app/apps/web/dist /usr/share/nginx/html
EXPOSE 80
