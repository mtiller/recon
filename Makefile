all:
	-mkdir test_output
	nosetests --with-coverage --cover-package=recon --cover-html

doctests:
	nosetests --with-doctest

comp: all
	ls -al dsres* tests/*.mat
