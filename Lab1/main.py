import time
import random
from pynput import keyboard

page_size = 4
total_physical_pages = 12
total_processes = 4
timeout = 20  # 20 milliseconds timeout
break_program = False


def on_press(key):
    global break_program
    if key == keyboard.Key.esc:
        print('[EXIT] ESC was pressed')
        break_program = True
        return False


class PageTableEntry:  # Virtual page
    def __init__(self, presence_bitP: bool, reference_bitR: bool, modification_bitM: bool, address: int, pid: int):
        self.presence_bitP = int(presence_bitP)
        self.reference_bitR = int(reference_bitR)
        self.modification_bitM = int(modification_bitM)
        self.address = address
        self.pid = pid

    def info(self):
        return f'Virtual Page: (P:{self.presence_bitP}, R:{self.reference_bitR}, M:{self.modification_bitM}, ' \
               f'Address:{self.address}, Process_id:{self.pid})'


class PhysicalPage:
    def __init__(self, entry: PageTableEntry, physical_address: int, last_usage_time: int):
        self.entry = entry
        self.physical_address = physical_address
        self.last_usage_time = last_usage_time

    def info(self):
        return f'Physical Page: ({self.entry.info()}, physical_address:{self.physical_address}, ' \
               f'last_appeal_time:{self.last_usage_time})'

    def __contains__(self, item):
        return self.entry == item


class MemoryManager:
    def __init__(self, physical_pages_quantity: int, page_size: int, timeout: int):
        self.physical_pages_quantity = physical_pages_quantity
        self.page_size = page_size
        self.timeout = timeout
        self.counter = 0
        self.physical_memory = []
        self.busy_physical_blocks = 0

    def demand_page(self, virtual_page):
        if virtual_page.presence_bitP:
            file.write('[demand_page] Page is now placed in memory')
            file.write('\n')
        else:
            file.write('[demand_page] Page fault. Freeing up place...')
            file.write('\n')
            if len(self.physical_memory) < self. physical_pages_quantity:
                self.physical_memory.append(PhysicalPage(virtual_page, self.busy_physical_blocks * self.page_size,
                                                         time.time_ns() // 1_000_000))  # time in milliseconds
                file.write('[demand_page] Page was successfully located to a new page block')
                file.write('\n')
                file.write(f'[demand_page] {self.physical_memory[self.busy_physical_blocks].info()}')
                file.write('\n')
                self.busy_physical_blocks += 1
            else:
                file.write('[demand_page] There is no free page block. Analyzing...')
                file.write('\n')

                # replacing page with NRU algorithm

                swapping = False
                class0 = []
                class1 = []
                class2 = []
                class3 = []
                self.counter = 0
                while self.counter < self.physical_pages_quantity:
                    if self.physical_memory[self.counter].entry.reference_bitR:
                        if time.time_ns() // 1_000_000 - self.physical_memory[self.counter].last_usage_time >= \
                                self.timeout:
                            self.physical_memory[self.counter].entry.reference_bitR = 0
                        else:
                            if self.physical_memory[self.counter].entry.modification_bitM:
                                # swap imitation
                                self.swapping()
                                class3.append(self.physical_memory[self.counter].entry)
                            else:
                                class2.append(self.physical_memory[self.counter].entry)
                    if not self.physical_memory[self.counter].entry.reference_bitR:
                        if self.physical_memory[self.counter].entry.modification_bitM:
                            # swap imitation
                            self.swapping()
                            class1.append(self.physical_memory[self.counter].entry)
                        else:
                            class0.append(self.physical_memory[self.counter].entry)
                    self.counter += 1
                if len(class0):
                    victim_page = random.choice(class0)
                elif len(class1):
                    victim_page = random.choice(class1)
                elif len(class2):
                    victim_page = random.choice(class2)
                elif len(class3):
                    victim_page = random.choice(class3)
                else:
                    raise Exception('Something went wrong')
                self.replace_page(victim_page, virtual_page)
            virtual_page.presence_bitP = 1
        virtual_page.reference_bitR = 1
        file.write('[demand_page] Accessing the page: ')
        if random.randint(1, 100) <= 50:
            virtual_page.modification_bitM = 1
            file.write('Writing')
            file.write('\n')
        else:
            virtual_page.modification_bitM = 0
            file.write('Reading')
            file.write('\n')
        file.write(f'[demand_page] {virtual_page.info()}')
        file.write('\n')

    def memory_dump(self):
        file.write('[memory_dump]' + '='*120)
        file.write('\n')
        for i in self.physical_memory:
            file.write(f'{i.info()}')
            file.write('\n')
        file.write('[memory_dump]' + '='*120)
        file.write('\n')
        file.write('\n')

    def replace_page(self, old_virtual_page: PageTableEntry, new_virtual_page: PageTableEntry):
        file.write(f'[demand_page] This page" {old_virtual_page.info()}"')
        file.write('\n')
        file.write(f'[demand_page] was changed to this page "{new_virtual_page.info()}"')
        file.write('\n')
        for i in range(len(self.physical_memory)):
            if old_virtual_page in self.physical_memory[i]:
                idx = i
                break
        self.physical_memory[idx].entry.presence_bitP = 0
        self.physical_memory[idx].entry = new_virtual_page
        self.physical_memory[idx].last_usage_time = time.time_ns() // 1_000_000

    def swapping(self):
        pass


class Process:
    def __init__(self, pid: int, memory_manager: MemoryManager):
        self.pid = pid
        self.memory_manager = memory_manager
        self.pages_quantity = random.randint(3, 12)
        self.virtual_memory = []
        self.create_process_pages()
        file.write(f'Created process {self.pid} with {self.pages_quantity} pages')
        file.write('\n')

    def create_process_pages(self):
        for i in range(self.pages_quantity):
            self.virtual_memory.append(PageTableEntry(False, False, False, i * page_size, self.pid))

    def get_page(self):
        file.write(f'[get_page] Process {self.pid} calls get_page()')
        file.write('\n')
        chance = random.randint(1, 100)
        # if chance < 90 then process uses his working set of virtual memory
        # WORKING SET IS THE FIRST QUADRANT (fourth part) of process pages
        if chance <= 90:
            page = self.virtual_memory[random.randint(0, round(self.pages_quantity / 4 - 1))]
            file.write(f'[get_page] Process {self.pid} accesses page from his working set')
            file.write('\n')
        else:
            page = self.virtual_memory[random.randint(round(self.pages_quantity / 4), self.pages_quantity - 1)]
            file.write(f'[get_page] Process {self.pid} accesses new page (not from his working set)')
            file.write('\n')
        file.write(f'[get_page] {page.info()}')
        file.write('\n')
        self.memory_manager.demand_page(page)


if __name__ == '__main__':
    with keyboard.Listener(on_press=on_press) as listener:
        with open('log.txt', 'w') as file:
            manager = MemoryManager(total_physical_pages, page_size, timeout)
            processes_lst = [Process(i + 1, manager) for i in range(total_processes)]
            file.write('\n')
            while not break_program:
                for i in processes_lst:
                    i.get_page()
                    file.write('\n')
                manager.memory_dump()
        listener.join()


# if __name__ == '__main__':
#     with open('log.txt', 'w') as file:
#         manager = MemoryManager(total_physical_pages, page_size, timeout)
#         processes_lst = [Process(i + 1, manager) for i in range(total_processes)]
#         file.write('\n')
#         for j in range(100):
#             for i in processes_lst:
#                 i.get_page()
#                 file.write('\n')
#             manager.memory_dump()

