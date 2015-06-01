TDAG_FILE_NAME=task_DAG.json
SVG_FILE_NAME=output.svg

all: clean temp.dot
	dot -Tsvg temp.dot -o $(SVG_FILE_NAME)

temp.dot:
	python process_task_dag.py $(TDAG_FILE_NAME) > temp.dot

clean:
	rm temp.dot output.svg
