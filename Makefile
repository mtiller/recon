all:
	nosetests --with-coverage --cover-package=recon --cover-html

comp: all
	ls -al dsres* tests/dsres*
