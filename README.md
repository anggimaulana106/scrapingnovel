# Fanmtl Novel Scraper

A comprehensive Scrapy project to scrape novel data from fanmtl.com and store it in PostgreSQL.

## Features

- Scrapes novel metadata, chapter lists, and chapter content
- Stores data in PostgreSQL with proper schema
- **Enhanced with proxy rotation and user agent rotation**
- Handles Cloudflare protection
- Implements rate limiting and retry mechanisms
- Provides detailed logging and monitoring

## Requirements

- Python 3.8+
- PostgreSQL 12+
- Required Python packages (see requirements.txt)
- A list of proxies in proxies.txt

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/fanmtl-scraper.git
cd fanmtl-scraper