all: clean lint
	./ukmohso-ingest.py -d -f ukmohso-ingest.yml

clean:
	rm -f *.txt.gz
lint:
	pycodestyle -v .
	pydocstyle -v .
