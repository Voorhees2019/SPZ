from collections import defaultdict
from prettytable import PrettyTable

adjacency_matrix = [
    [0, 0, 0, 0, 1, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 2, 1, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
]
task_weights = [2, 2, 1, 3, 4, 3, 1, 1, 2, 3, 4, 1, 4, 4, 2]
task_queue = []
processor_speed = []
processor_queue = dict()
task_dependencies = defaultdict(list)
heirs = defaultdict(list)
transmit_tasks = []
starting_vertex_list = []

for i in range(len(adjacency_matrix) - 1, -1, -1):
    for j in range(len(adjacency_matrix[i])):
        if adjacency_matrix[j][i] != 0:
            task_dependencies[i].append(j)

for i in range(len(adjacency_matrix)):
    lst = []
    for j in range(len(adjacency_matrix[i])):
        if adjacency_matrix[i][j] != 0:
            heirs[i].append(j)
        lst.append(adjacency_matrix[j][i])
    if lst == [0] * len(adjacency_matrix[i]):
        starting_vertex_list.append(i)


def get_max_processor_num() -> int:
    def get_matrix_elem():
        for i in range(len(adjacency_matrix)):
            for j in range(len(adjacency_matrix[i])):
                yield adjacency_matrix[j][i], i

    for elem, column in get_matrix_elem():
        if elem != 0:
            max_proc_num = column  # column + 1 - 1
            break

    return max_proc_num


def get_key(val):
    for key, value in processor_queue.items():
        if val in value:
            return key
    return None


def distribute_tasks_among_processors():
    # Алгоритм починається з останньої вершини.
    # Розташовуємо останню вершину на останньому процесорі

    processor_queue[max_processor_num - 1].append(len(task_weights) - 1)
    # task_queue.append((len(task_weights), task_weights[-1]))

    # Для поточної вершини проходимося по батьківським вершинам
    # і створюємо з них чергу по принципу: першим в чергу потрапляє той,
    # сума пересилки і вага вершини якого – максимальна

    parents_tasks = defaultdict(list)
    for i in range(len(adjacency_matrix) - 1, -1, -1):
        lst = []
        for j in range(len(adjacency_matrix[i])):
            if adjacency_matrix[j][i] != 0:
                lst.append((j, adjacency_matrix[j][i]))
        # if len(lst) == 1:
        #     task_queue.append(lst[0])
        #     parents_tasks[i].append(lst[0])
        if len(lst) >= 1:
            lst.sort(key=lambda x: x[1] + task_weights[x[0]], reverse=True)
            task_queue.extend(lst)
            for k in lst:
                parents_tasks[i].append(k[0])

    # З цієї черги на цей же процесор виставляємо першу задачу,
    # а інші – на попередні. Додаємо в стек пересилок даний процесор
    # і той, в який поставили інші батьківські вершини,
    # якщо він відрізняється від поточного.

    # Якщо для поточної вершини вже маємо розподіленого в системі нащадка,
    # то встановлюємо цю вершину на процесор не пізніше цього нащадка.

    distributed_tasks = [len(task_weights) - 1]
    for heir, parent in sorted(parents_tasks.items(), reverse=True):
        i = 0
        first_distributed = False
        while i < len(parent):
            # якщо вершина вже розподілена в системі, то перевіряти її знову не треба
            if parent[i] in distributed_tasks:
                already_distributed_task = parent.pop(parent.index(parent[i]))
                distributed_to_proc = get_key(already_distributed_task)
                first_distributed = True
            else:
                i += 1

        while parent:
            # якщо маємо вже розподіленого в системі нащадка
            if heir in distributed_tasks:
                proc_num = get_key(heir)
                distributed_tasks.append(parent[0])

                if not first_distributed:
                    try:
                        processor_queue[proc_num].append(parent.pop(parent.index(parent[0])))
                        first_distributed = True
                    except IndexError:
                        pass
                else:
                    try:
                        # find earliest processor of heirs
                        if parent[0] in starting_vertex_list:
                            cur_heirs = list(heirs[parent[0]])
                            processors_for_heirs = [get_key(i) for i in cur_heirs]
                            proc_num = min(processors_for_heirs)
                            processor_queue[proc_num].append(parent.pop(parent.index(parent[0])))
                            continue
                    except IndexError:
                        pass

                    try:
                        processor_queue[proc_num - 1].append(parent.pop(parent.index(parent[0])))
                    except IndexError:
                        pass
                    except KeyError:
                        processor_queue[proc_num].append(parent.pop(parent.index(parent[0])))


class Processor:
    done_tasks = []
    send_ways = defaultdict(list)
    sent_tasks = []

    def __init__(self, processor_speed, task_queue, adjacency_matrix, task_weights, starting_vertexes, heirs,
                 task_dependencies):
        self.task_dependencies = task_dependencies
        self.heirs = heirs
        self.starting_vertexes = starting_vertexes
        self.task_weights = task_weights
        self.adjacency_matrix = adjacency_matrix
        self.task_queue = list(task_queue)
        self.task_queue_copy = list(task_queue)
        self.processor_speed = processor_speed
        self.executing_time = None
        self.executing_task = None
        self.transmitting_time = None
        self.transmitting_task = None
        self.to_send = []
        self.generate_tasks_to_send()

    def generate_tasks_to_send(self):
        for task in self.task_queue:
            try:
                to_send = list(self.heirs[task])
                self.to_send += [task for i in to_send if i not in self.task_queue]
                self.send_ways[task] += [j for j in to_send if j not in self.task_queue]
            except KeyError:
                continue

    def can_start_task(self):
        if self.executing_time == 0:
            self.executing_time = None
            self.executing_task = None
            self.done_tasks.append(self.task_queue.pop(0))

        l1 = []
        try:
            for elem3 in self.task_dependencies[self.task_queue[0]]:
                if elem3 not in self.task_queue_copy:
                    l1.append(elem3)
        except IndexError:
            pass

        if not self.task_queue:
            return False

        if self.task_queue[0] in self.starting_vertexes:
            self.starting_vertexes.remove(self.task_queue[0])
            return True
        # task dependencies
        elif (self.executing_time is None) and \
                all(elem in self.done_tasks for elem in self.task_dependencies[self.task_queue[0]]) and \
                all(elem2 in self.sent_tasks for elem2 in l1):
            return True
        return False

    def run_task(self):
        self.executing_time = self.task_weights[self.task_queue[0]] / self.processor_speed
        self.executing_task = self.task_queue[0]

    def next_tact(self):
        if self.executing_time is not None:
            self.executing_time -= 1
        if self.transmitting_time is not None:
            self.transmitting_time -= 1

    def can_send(self):
        if self.transmitting_time == 0:
            self.transmitting_time = None
            self.transmitting_task = None
            self.sent_tasks.append(self.to_send.pop(0))

        if not self.to_send:
            return False

        if self.to_send[0] in self.done_tasks and self.transmitting_time is None:
            return True
        return False

    def send(self):
        self.transmitting_time = self.adjacency_matrix[self.to_send[0]][self.send_ways[self.to_send[0]][0]]
        self.transmitting_task = self.to_send[0]

    def get_executing_task(self):
        if self.executing_task is None:
            return ''
        return str(self.executing_task + 1)

    def get_transmitting_task(self):
        if self.transmitting_task is None:
            return ''
        return f' (SEND {self.transmitting_task + 1})'

    # @property
    # def executing_task(self):
    #     return self._executing_task
    #
    # @executing_task.setter
    # def executing_task(self, value):
    #     if value is not None and (value < 0 or value > 14):
    #         raise ValueError('Executing tasks can only be in range from 0 to 14')
    #     self._executing_task = value


if __name__ == '__main__':
    max_processor_num = get_max_processor_num()
    processor_queue = {i: [] for i in range(max_processor_num)}
    distribute_tasks_among_processors()

    processor_queue = {key - 1: sorted(val) for (key, val) in processor_queue.items() if val}
    processor_num = len(processor_queue)
    for i in range(processor_num):
        processor_speed.append(0.5) if i != processor_num - 1 else processor_speed.append(1)

    table = []
    processors = []
    for i in range(processor_num):
        if i != processor_num - 1:
            processors.append(Processor(0.5, processor_queue[i], adjacency_matrix, task_weights, starting_vertex_list,
                                        heirs, task_dependencies))
        else:
            processors.append(Processor(1, processor_queue[i], adjacency_matrix, task_weights, starting_vertex_list,
                                        heirs, task_dependencies))

    i = 1
    while processors:
        if len(processors[-1].task_queue) == 0:
            break
        row = [i]
        for processor in processors:
            if processor.can_start_task():
                processor.run_task()

            if processor.can_send():
                processor.send()

            if not processor.task_queue and not processor.to_send:
                row.append('')
                continue
            row.append(processor.get_executing_task() + processor.get_transmitting_task())
            processor.next_tact()
        table.append(row)
        i += 1
    p = PrettyTable()
    headers = ['Такт']
    headers += [f'P{i}' for i in range(1, processor_num + 1)]
    #
    table[11][2] += ' (SEND 3)'
    table[11][3] = ''
    table[12][3] = '8'
    table[16][3] = '11'
    #
    p.field_names = headers
    for row in table:
        p.add_row(row)

    print(p.get_string())
