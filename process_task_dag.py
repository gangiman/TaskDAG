import json
import sys
from operator import attrgetter
from collections import deque

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


class DAG:
    def __init__(self):
        pass


class TaskDAG(DAG):

    def __init__(self):
        DAG.__init__(self)
        self.names_to_obj = {}

    @classmethod
    def from_json(cls, filename):
        with open(filename, "r") as fh:
            tasks_structure = json.loads(fh.read())
        task_dag = cls()
        task_dag.raw_structure = tasks_structure
        sorted_tasks = topological_sort(tasks_structure)
        sorted_tasks.reverse()
        for task_name in sorted_tasks:
            task_body = tasks_structure[task_name]
            task = task_dag.add_task(task_name, task_body)
            if "deps" in task_body:
                for dep in task_body["deps"]:
                    task.add_dependency(task_dag.names_to_obj[dep])
        task_dag.remove_inactive_tasks()
        return task_dag

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
                params=
                (
                    " | " + " | ".join("{{{0} | {1}}}".format(k, v)
                                       for k, v in task_body["data"].items())
                ).replace(">", "\>")
                if "data" in task_body else
                ""
            )
            if "deps" in task_body:
                for dep in task_body["deps"]:
                    dot_code += "\n{0} -> {1};".format(task_names_to_ids_mapping[dep], task_names_to_ids_mapping[task_name])
        return template.format(dot_code)


def topological_sort(graph):
    order, enter, state = deque(), set(graph), {}

    def dfs(node):
        state[node] = GRAY
        # deps = graph.get(node, ())
        deps = graph[node]["deps"] if "deps" in graph[node] else ()
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


def main(filename):
    task_dag = TaskDAG.from_json(filename)
    task_dag.remove_inactive_tasks()
    # print("\n".join(str(t) for t in task_dag.find_final_tasks()))
    print(task_dag.translate_to_dot())


if __name__ == '__main__':
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("path to task_dag.json needed.")
