VERSION = 0.1.0

all: clean lint
	./ukmohso-ingest.py -f ukmohso-ingest.yml

changelog:
	sed "s/^unreleased_version_label.*/unreleased_version_label = '$(VERSION)'/" \
	    .gitchangelog.rc > .gitchangelog.rc.new
	mv .gitchangelog.rc.new .gitchangelog.rc
	gitchangelog > CHANGELOG.md

clean:
	rm -f *.txt.gz
lint:
	pycodestyle -v .
	pydocstyle -v .
