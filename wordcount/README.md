# Word Count

## Setup

### Environment

```
python3 -m venv .
```
```
source ./bin/activate
```

### Install dependencies

```
pip install wheel
```
```
pip install 'apache-beam[gcp]'
```

## Run

```
python3 wordcount.py --input=kinglear.txt --output=output
```