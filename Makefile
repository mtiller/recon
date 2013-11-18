all:
	nosetests --with-coverage --cover-package=recon --cover-html
	nosetests --with-doctest

comp: all
	ls -al dsres* tests/*.mat
