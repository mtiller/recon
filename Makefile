all:
	-mkdir test_output
	nosetests --with-coverage --cover-package=recon --cover-html

prof:
	-mkdir test_output
	nosetests --with-profile tests/TestPerf.py --profile-sort tottime 2> test_output/prof.out

doctests:
	nosetests --with-doctest

comp: all
	ls -al dsres* tests/*.mat
