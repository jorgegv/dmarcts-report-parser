.SILENT:
.PHONY: tidy

tidy:
	perltidy -pro=perltidyrc *.pl
