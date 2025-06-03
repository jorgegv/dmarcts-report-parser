.SILENT:
.PHONY: tidy

tidy:
	perltidy -pro=perltidyrc *.pl

syntax:
	source ./set-perl-env.sh && for i in *.pl; do perl -c "$$i"; done
