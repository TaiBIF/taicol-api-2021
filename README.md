# TaiCOL API

This is repository for [TaiCOL API v2](https://taicol.tw/api) developed based on [name tool](https://github.com/TaiBIF/taicol-2021) database schema.

---
## Deployment

1. copy `dotenv.example` file and rename it to `.env`
2. run `docker-compose build` to build docker images
3. run `docker-compose up -d` to start API service

> **Note**
> To run [TaiCOL API v1](https://github.com/TaiBIF/taicol-test) and v2 at the same time, follow the following steps:
> 1. run v1 on port 8001 and v2 on port 8002 docker separately
> 2. move `scripts/custom_nginx.conf` to `/etc/nginx/conf.d/`
> 3. restart nginx
> ```
> sudo systemctl restart nginx
> ```
> 4. check nginx status
> ```
> systemctl status nginx.service
> ```
> 5. if https is required, install certbot and initialize it
> ```
> sudo amazon-linux-extras install epel
> sudo yum install certbot-nginx
> sudo certbot --nginx
> ```

---
## Documentation

- [taicol.tw](https://taicol.tw/api)
