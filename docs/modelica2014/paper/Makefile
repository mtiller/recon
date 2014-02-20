all: recon.pdf abstract.pdf

recon.pdf: recon.tex recon.bib
	pdflatex recon.tex
	bibtex recon
	pdflatex recon.tex

clean:
	-rm *.aux *.bbl *.blg *-blx.bib *.log
	-rm *.out *.run.xml *.pdf *.ps *.bcf *.fdb_latexmk
	-rm *.fls *.dvi

abstract.pdf: abstract.tex recon.bib
	pdflatex abstract.tex
	bibtex abstract
	pdflatex abstract.tex
