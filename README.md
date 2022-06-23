# Datasets Downloader

Download a dataset by web scraping.

Currently supports download and unziping of the datasets: [SiW](#siw) | [S3DFM](#s3dfm)

## SiW

Download and unzip the SiW dataset using threads to folders ```./siw_ziped/``` and ```./siw_unziped/```

```bash
python dataset_builder.py siw --user=<provided-by-the-siw-authors> --pwd=<provided-by-the-siw-authors> -tu
```

## S3DFM

Download and unzip the S3DFM dataset to folders ```./s3dfm_ziped/``` and ```./s3dfm_unziped/```

```bash
python dataset_builder.py s3dfm -du
```

### USAGE

```bash
python dataset_builder.py -h
```

```text
usage: dataset_builder.py [-h] [--user USER] [--pwd PASSWORD]
                          [--zdir ZIPED_DIR] [--uzdir UNZIPED_DIR] [-t] [-d]
                          [-u] [-n]
                          {siw,s3dfm}

positional arguments:
  {siw,s3dfm}           Builder

optional arguments:
  -h, --help            show this help message and exit
  --user USER           User
  --pwd PASSWORD, --password PASSWORD
                        Password
  --zdir ZIPED_DIR, --ziped_dir ZIPED_DIR
                        Ziped directory
  --uzdir UNZIPED_DIR, --unziped_dir UNZIPED_DIR
                        UnZiped directory
  -t, --threads_download
                        Download the dataset using threads
  -d, --download        Download the dataset in sequential order
  -u, --unzip           Unzip the dataset
  -n, --notif, --notification
                        Send notification on error, or on download and unzip
                        end
```
