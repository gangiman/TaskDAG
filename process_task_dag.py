import json
import sys
import os
from operator import attrgetter
from collections import deque
import argparse


from yaml import load, dump
from pprint import pprint


try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper



def get_arguments():
    parser = argparse.ArgumentParser(description='Utility to work with task TAG')

    def is_valid_file(arg):
        if not os.path.exists(arg):
            parser.error("The file %s does not exist!" % arg)
        else:
            return open(arg, 'r')  # return an open file handle

    parser.add_argument("-i", dest="input", required=True,
                        help="input taskDAG file", metavar="FILE",
                        type=is_valid_file)
    parser.add_argument('--orphans', action='store_true', help='dot file to write graph vector image')
    parser.add_argument('-o', dest="dot_output", metavar="FILE.dot", help='dot file to write graph vector image')
    return parser.parse_args()


GRAY, BLACK = 0, 1

template = """digraph graphname {{
rankdir=LR;
node[shape=record];
{0}
}}"""


class Task:

    def __init__(self, name, body):
        self.name = name
        self._raw = body
        self.data = body.get("data", {})
        self.dependencies = []
        self.predications = []

    def add_dependency(self, other_task):
        self.dependencies.append(other_task)
        other_task.predications.append(self)

    def add_predication(self, other_task):
        self.predications.append(other_task)
        other_task.dependencies.append(self)

    def __repr__(self):
        return "Task: ({0})".format(self.name)

    @property
    def depends(self):
        return bool(self.dependencies)

    @property
    def predicts(self):
        return bool(self.predications)

    def delete(self):
        for predicate in self.predications:
            predicate.dependencies.remove(self)
        for dependant in self.dependencies:
            dependant.predications.remove(self)


class TaskDAG(object):

    def __init__(self, tasks_structure):
        self.names_to_obj = {}
        # task_dag.raw_structure = tasks_structure
        sorted_tasks = topological_sort(tasks_structure)
        sorted_tasks.reverse()
        for task_name in sorted_tasks:
            task_body = tasks_structure.get(task_name, {})
            task = self.add_task(task_name, task_body)
            if "deps" in task_body:
                for dep in task_body["deps"]:
                    task.add_dependency(self.names_to_obj[dep])
        self.remove_inactive_tasks()

    def get_all_tasks(self):
        return self.names_to_obj.values()

    def add_task(self, task_name, task_body):
        temp_task = Task(task_name, task_body)
        self.names_to_obj[task_name] = temp_task
        return temp_task

    def find_current_tasks(self):
        return [
            task for task in self.get_all_tasks()
            if not task.depends
        ]

    def find_final_tasks(self):
        return [
            task for task in self.get_all_tasks()
            if not task.predicts
        ]

    def delete_task(self, task):
        task.delete()
        del self.names_to_obj[task.name]

    def remove_inactive_tasks(self):
        inactive_tasks = set()

        def go_down(_task):
            all_tasks = []
            for dep in _task.dependencies:
                all_tasks.extend(go_down(dep))
            all_tasks.append(_task)
            return all_tasks
        for task in self.names_to_obj.values():
            if task.data.get("status", "") in ("done", "failed"):
                inactive_tasks.update(go_down(task))
        for task in inactive_tasks:
            self.delete_task(task)

    def to_dict(self):
        return {
            tname: {
                "deps": list(map(attrgetter("name"), task.dependencies)),
                "data": task.data
            } for tname, task in self.names_to_obj.items()
        }

    def print_final_tasks(self):
        final_tasks = self.find_final_tasks()
        print("\n".join(str(t) for t in final_tasks))

    def translate_to_dot(self):
        # create mapping from task names to labels for dot file
        # task_dag = self.raw_structure
        task_dag = self.to_dict()
        task_names_to_ids_mapping = {task_name: "task_{0}".format(i) for i, task_name in enumerate(task_dag.keys())}
        dot_code = ""
        # iterating over task name (task_name) and task body (task_body) which is object
        # with two possible keys "data" (task parameters and values) and
        # "deps" (names of task which current one depends on)
        for task_name, task_body in task_dag.items():
            dot_code += '\n{id_} [label="{label}{params}" shape=Mrecord]'.format(
                id_=task_names_to_ids_mapping[task_name],
                label=task_name,
                params=(
                    " | " + " | ".join("{{{0} | {1}}}".format(k, v)
                                       for k, v in task_body["data"].items())
                ).replace(">", "\>")
                if "data" in task_body else
                "")
            if "deps" in task_body:
                for dep in task_body["deps"]:
                    dot_code += "\n{0} -> {1};".format(task_names_to_ids_mapping[dep], task_names_to_ids_mapping[task_name])
        return template.format(dot_code)


def topological_sort(graph):
    order, enter, state = deque(), set(graph), {}

    def dfs(node):
        state[node] = GRAY
        if node in graph and "deps" in graph[node]:
            deps = graph[node]["deps"]
        else:
            deps = ()
        for k in deps:
            sk = state.get(k, None)
            if sk == GRAY:
                raise ValueError("Error. Graph contains cycle. Not a DAG.")
            if sk == BLACK:
                continue
            enter.discard(k)
            dfs(k)
        order.appendleft(node)
        state[node] = BLACK
    while enter:
        dfs(enter.pop())
    return order


def validate_task_dag(data):
    assert isinstance(data, dict)
    for _task in data.values():
        assert isinstance(_task, dict)
        for _key in _task:
            assert _key in ("deps", "data")


def print_orphan_nodes(data):
    deps_all = set(sum([task.get('deps', []) for task in data.values()], []))
    for _task in data:
        if _task not in deps_all:
            print(_task)

# # output = dump(data, Dumper=Dumper)


def main():
    args = get_arguments()
    data = load(args.input, Loader=Loader)
    validate_task_dag(data)
    if args.orphans:
        print_orphan_nodes(data)
    if args.dot_output:
        with open(args.dot_output, 'w+') as output_stream:
            td = TaskDAG(data)
            output_stream.write(td.translate_to_dot())


if __name__ == '__main__':
    main()
