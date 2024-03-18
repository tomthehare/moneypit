test:
	echo "Testing it"

exec:
	python3 main.py

lock-requirements:
	pip freeze --disable-pip-version-check >> requirements.lock

install-requirements:
	python3 environment/install_requirements.py