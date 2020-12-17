import os
import pickle
import random


class BlockEntry:
    def __init__(self, block_size: 'int mod4==0'):
        self.block_size = (block_size // 4) * 4
        self.block_value = ['_'] * block_size


class Descriptor:
    def __init__(self, file_type: 'str file or directory', name: str, file_size: int, memory_location: list):
        self.file_type = file_type
        self.links = [name]
        self.file_size = file_size
        self.memory_location = memory_location
        self.fd = None


class FileSystem:
    def __init__(self, block_quantity: 'int > 0', max_descriptors_num: 'int > 0'):
        self.block_quantity = block_quantity
        self.max_descriptors_num = max_descriptors_num
        self.memory = []
        self.bit_map = [0 for _ in range(self.block_quantity)]  # 0 - free, 1 - busy
        self.descriptors = []
        self.create_memory()
        self.fds = set()  # check if the random function returns the same values in a row for 'descriptor.fd'

    def create_memory(self):
        block_size = 4
        for i in range(self.block_quantity):
            self.memory.append(BlockEntry(block_size))

    def filestat_command(self, id: int):
        try:
            descriptor = self.descriptors[id]
            print(f'Descriptor {id}: file_type:{descriptor.file_type}, links:{descriptor.links}, '
                  f'file_size:{descriptor.file_size}, memory_location:{descriptor.memory_location}')
        except IndexError:
            print(f'Descriptor id "{id}" doesn\'t exist')

    def ls_command(self):
        for descriptor_id in range(len(self.descriptors)):
            for link in self.descriptors[descriptor_id].links:
                print(f'File: "{link}", descriptor_id: {descriptor_id}')

    def create_command(self, name):
        size = random.randint(1, 4)
        if self.bit_map.count(0) < size:
            print(f'[ERROR] Unable to create the file, not enough memory')
            return
        if len(self.descriptors) > self.max_descriptors_num:
            print(f'[ERROR] Unable to create the file, exceeded the max number of descriptors')
            return
        for descriptor_id in range(len(self.descriptors)):
            for link in self.descriptors[descriptor_id].links:
                if link == name:
                    print(f'[ERROR] Unable to create the file, this file already exists')
                    return

        # place the file in memory
        s = size
        location = []

        for i in range(len(self.bit_map)):
            if self.bit_map[i] == 0:
                self.memory[i].block_value = ['#'] * self.memory[0].block_size  # filling the block with '#'
                s -= 1
                self.bit_map[i] = 1
                location.append(i)
            if not s:
                break

        # create a descriptor
        self.descriptors.append(Descriptor('file', name, size, location))
        print(f'"{name}" has been created')

    def open_command(self, name):
        all_links = []
        for descriptor_id in range(len(self.descriptors)):
            for link in self.descriptors[descriptor_id].links:
                all_links.append(link)
                if link == name:
                    r = random.randint(1000, 5000)
                    while r in self.fds:
                        r = random.randint(1000, 5000)
                    self.descriptors[descriptor_id].fd = r
                    self.fds.add(r)
                    print(f'File "{name}" has been opened, fd: "{self.descriptors[descriptor_id].fd}"')
                    return

        if name not in all_links:
            print(f'[ERROR] File doesn\'t exist')

    def close_command(self, fd):
        fds = []
        for descriptor in self.descriptors:
            fds.append(descriptor.fd)
            if descriptor.fd == fd:
                descriptor.fd = None
                print(f'File with fd:{fd} has been closed')
                return

        if fd not in fds:
            print(f'[ERROR] There is no opened file with such fd: "{fd}"')

    def read_command(self, fd, offset, size):
        fds = []
        for descriptor in self.descriptors:
            fds.append(descriptor.fd)
            if descriptor.fd == fd:
                # check the offset
                if offset > len(descriptor.memory_location):
                    print(f'[ERROR] Can not read. The offset is too large')
                else:
                    print(' '.join(self.memory[descriptor.memory_location[0] + offset].block_value[:size]))
                    # print(' '.join(self.memory[descriptor.memory_location[offset]].block_value[:size]))
                return

        if fd not in fds:
            print(f'[ERROR] Can not read. There is no opened file with such fd: "{fd}"')

    def write_command(self, fd, offset, size):
        fds = []
        for descriptor in self.descriptors:
            fds.append(descriptor.fd)
            if descriptor.fd == fd:
                # check the offset
                if offset > len(descriptor.memory_location):
                    print(f'[ERROR] Can not write. The offset is too large')
                else:
                    # check the data size
                    if size > self.memory[0].block_size:
                        print(f'[ERROR] Can not write. Data size is too large')
                    else:
                        print(f'Before writing: '
                              f'{self.memory[descriptor.memory_location[0] + offset].block_value}')
                        self.memory[descriptor.memory_location[0] + offset].block_value[:size] = ['W'] * size
                        # self.memory[descriptor.memory_location[offset]].block_value[:size] = ['W'] * size
                        print(f'After writing: '
                              f'{self.memory[descriptor.memory_location[0] + offset].block_value}')
                return

        if fd not in fds:
            print(f'[ERROR] Can not write. There is no opened file with such fd: "{fd}"')

    def link_command(self, name1: 'existing file', name2: 'new link name'):
        all_links = [link for d in self.descriptors for link in d.links]

        if name1 not in all_links:
            print(f'[ERROR] File "{name1}" doesn\'t exist')
            return

        for descriptor in self.descriptors:
            for link in descriptor.links:
                if link == name1:
                    if name2 in descriptor.links:
                        print(f'[ERROR] The link "{name2}" already exists')
                        return
                    elif name2 in all_links:
                        print(f'[ERROR] This link "{name2}" is already busy')
                        return
                    if name2 not in descriptor.links:
                        descriptor.links.append(name2)

    def unlink_command(self, name):
        all_links = [link for d in self.descriptors for link in d.links]

        if name not in all_links:
            print(f'[ERROR] There is no such link in FileSystem: "{name}"')
            return

        for descriptor_id in range(len(self.descriptors)):
            for link in self.descriptors[descriptor_id].links:
                if link == name:
                    if len(self.descriptors[descriptor_id].links) > 1:
                        self.descriptors[descriptor_id].links.remove(name)
                        print(f'The link "{name}" was deleted')
                    # check if there is only one link in links -> del the descriptor and free up the memory and bit_map
                    elif len(self.descriptors[descriptor_id].links) == 1:
                        for block_id in self.descriptors[descriptor_id].memory_location:
                            self.memory[block_id].block_value = ['_'] * self.memory[0].block_size
                            self.bit_map[block_id] = 0
                        del self.descriptors[descriptor_id]
                        print(f'Descriptor has been deleted. There was only one link for this file.')
                    return

    def truncate_command(self, name, size: 'only [1, 2, 3, 4] values are valid here'):
        all_links = [link for d in self.descriptors for link in d.links]

        if name not in all_links:
            print(f'[ERROR] There is no such link in FileSystem: "{name}"')
            return

        if size < 1 or size > 4:
            print(f'[ERROR] Inappropriate size value "{size}". '
                  f'In this FileSystem allowed only range from 1 to 4 blocks for the file size')
            return

        for descriptor_id in range(len(self.descriptors)):
            for link in self.descriptors[descriptor_id].links:
                if link == name:
                    # not to change if the given size is the same
                    if len(self.descriptors[descriptor_id].memory_location) == size:
                        print(f'Nothing has been changed. The given size is equal to the old one')
                        return

                    old_size = len(self.descriptors[descriptor_id].memory_location)

                    # to reduce the file
                    if size < old_size:
                        popped = []
                        popped.extend(self.descriptors[descriptor_id].memory_location[size:])
                        del self.descriptors[descriptor_id].memory_location[size:]
                        # free up the memory and bit_map
                        for i in popped:
                            self.memory[i].block_value = ['_'] * self.memory[0].block_size
                            self.bit_map[i] = 0
                        self.descriptors[descriptor_id].file_size = size

                    # to expand the file
                    elif size > old_size:
                        if self.bit_map.count(0) < size - old_size:
                            print(f'[ERROR] Unable to truncate the file, not enough memory')
                            return
                        else:
                            s = size - old_size
                            for i in range(len(self.bit_map)):
                                if self.bit_map[i] == 0:
                                    self.memory[i].block_value = ['0'] * self.memory[0].block_size
                                    s -= 1
                                    self.bit_map[i] = 1
                                    self.descriptors[descriptor_id].memory_location.append(i)
                                if not s:
                                    break
                            self.descriptors[descriptor_id].file_size = size

                    print(f'File {name} took {old_size} block(s)')
                    print(f'Now file {name} takes {size} block(s)')


if __name__ == '__main__':
    mounted = False
    while True:
        user_input = input('~$ ').split()
        # mount unmount ls
        if len(user_input) == 1:
            if user_input[0] == 'mount':
                if os.path.exists('filesystem.pickle'):
                    with open('filesystem.pickle', 'rb') as file:
                        fs = pickle.load(file)
                else:
                    fs = FileSystem(20, 20)
                mounted = True
                print(f'FileSystem was successfully mounted')
            elif user_input[0] == 'unmount':
                try:
                    with open('filesystem.pickle', 'wb') as file:
                        pickle.dump(fs, file)
                    del fs
                    mounted = False
                    print(f'FileSystem was successfully unmounted')
                except NameError:
                    print(f'[ERROR] FileSystem wasn\'t mounted before')
            elif user_input[0] == 'ls':
                try:
                    fs.ls_command()
                except NameError:
                    pass
            elif (user_input[0] == 'filestat' or user_input[0] == 'create' or user_input[0] == 'open' or
                  user_input[0] == 'close' or user_input[0] == 'unlink' or user_input[0] == 'link' or
                  user_input[0] == 'truncate' or user_input[0] == 'read' or user_input[0] == 'write') and mounted:
                print(f'Not enough arguments')

        # filestat create open close unlink
        elif len(user_input) == 2:
            if user_input[0] == 'filestat':
                try:
                    fs.filestat_command(int(user_input[1]))
                except NameError:
                    pass
                except ValueError:
                    print(f'[ERROR] Incorrect file_id. Expected "int" got "{user_input[1]}"')
            elif user_input[0] == 'create':
                try:
                    fs.create_command(user_input[1])
                except NameError:
                    pass
            elif user_input[0] == 'open':
                try:
                    fs.open_command(user_input[1])
                except NameError:
                    pass
            elif user_input[0] == 'close':
                try:
                    fs.close_command(int(user_input[1]))
                except NameError:
                    pass
                except ValueError:
                    print(f'[ERROR] Incorrect fd. Expected "int" got "{user_input[1]}"')
            elif user_input[0] == 'unlink':
                try:
                    fs.unlink_command(user_input[1])
                except NameError:
                    pass
            elif (user_input[0] == 'link' or user_input[0] == 'truncate' or user_input[0] == 'read'
                  or user_input[0] == 'write') and mounted:
                print(f'Not enough arguments')

        # link truncate
        elif len(user_input) == 3:
            if user_input[0] == 'link':
                try:
                    fs.link_command(user_input[1], user_input[2])
                except NameError:
                    pass
            elif user_input[0] == 'truncate':
                try:
                    fs.truncate_command(user_input[1], int(user_input[2]))
                except NameError:
                    pass
                except ValueError:
                    print(f'[ERROR] Incorrect argument: "{user_input[2]}"')
            elif (user_input[0] == 'read' or user_input[0] == 'write') and mounted:
                print(f'Not enough arguments')

        # read write
        elif len(user_input) == 4:
            if user_input[0] == 'read':
                try:
                    fs.read_command(int(user_input[1]), int(user_input[2]), int(user_input[3]))
                except NameError:
                    pass
                except ValueError:
                    print(f'[ERROR] Incorrect arguments for reading')
            elif user_input[0] == 'write':
                try:
                    fs.write_command(int(user_input[1]), int(user_input[2]), int(user_input[3]))
                except NameError:
                    pass
                except ValueError:
                    print(f'[ERROR] Incorrect arguments for writing')
