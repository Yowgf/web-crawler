# Web crawler

Polite web crawler written in Python.

## Disclaimer

This implementation contains an efficiency bug. As the program crawls more and
more URLs, some threads dangle. Therefore, if the number of URLs to be crawled
is 100,000, the program will have degraded performance as it reaches the goal of
100,000 pages. It might even stop crawling completely.

## Execution instructions

The following command will crawl 10000 pages in debug mode. In debug mode, the
program will output a JSON document containing a summary of every page crawled.

```shell
python3 main.py -s seeds.txt -n 10000 -d
```
