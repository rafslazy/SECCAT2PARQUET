# CAT → Parquet Extractor for Spanish Cadastral CAT files

### Spanish Cadastral (.CAT) Fixed-Width to Parquet CLI Tool



CLI tool to transform Spanish Cadastral (.CAT) fixed-width files into optimized Parquet datasets using PyArrow.



## Features



- Chunked streaming processing (memory efficient)

- Fixed-width parsing based on official cadastral specifications

- Schema consistency enforced via PyArrow

- CLI-driven execution (argparse-based)

- Snappy-compressed Parquet output

- Designed for large-scale national datasets



## Installation



git clone https://github.com/miguelfreb/SECCAT2PARQUET.git

cd SECCAT2PARQUET

pip install -r requirements.txt



## Usage



python src/cat\_extract.py --input-folder /path/2/cat --table 11 --output table11.parquet



## Supported Tables



All tables available from SEC services are supported:



11, 13, 14, 15, 16, 17



Structured according to official documentation:

https://www.catastro.hacienda.gob.es/documentos/formatos\_intercambio/catastro\_fin\_cat\_2006.pdf



## Performance


Tested on full national dataset:

~250GB raw CAT files to ~9GB optimized Parquet output

Stable memory footprint due to chunked streaming


## Author



Miguel Ángel Freijedo
