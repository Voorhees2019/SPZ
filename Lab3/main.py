import os
import pickle
import random


class BlockEntry:
    def __init__(self, block_size: 'int mod4==0'):
        self.block_size = (block_size // 4) * 4
        self.block_value = ['_'] * block_size


class File:
    def __init__(self, name, descriptor_id):
        self.links = [name]
        self.descriptor_id = descriptor_id


class Directory:
    def __init__(self, name, descriptor_id):
        self.links = [name]
        self.descriptor_id = descriptor_id
        self.content = []


class Symlink:
    def __init__(self, name, descriptor_id, path):
        self.links = [name]
        self.descriptor_id = descriptor_id
        self.path = path


class Descriptor:
    def __init__(self, file_type: 'str file or directory', file_size: int, memory_location: list):
        self.file_type = file_type
        self.links = 1
        self.file_size = file_size
        self.memory_location = memory_location
        self.fd = None


class FileSystem:
    def __init__(self, block_quantity: 'int > 0', max_descriptors_num: 'int > 0'):
        self.block_quantity = block_quantity
        self.max_descriptors_num = max_descriptors_num
        self.memory = []
        self.create_memory()
        self.bit_map = [0 for _ in range(self.block_quantity)]  # 0 - free, 1 - busy
        self.descriptors = []
        self.content = []
        self.fds = set()  # check if the random function returns the same values in a row for 'descriptor.fd'
        self.cur_dir = None
        self.initialize_root()

    def create_memory(self):
        block_size = 4
        for i in range(self.block_quantity):
            self.memory.append(BlockEntry(block_size))

    def initialize_root(self):
        self.descriptors.extend([Descriptor('directory', 0, []),
                                 Descriptor('symlink', 0, []),
                                 Descriptor('symlink', 0, [])])
        self.content.append(Directory('/', 0))
        self.content[0].content = [Symlink('..', 1, '/'),
                                   Symlink('.', 2, '/')]
        self.cur_dir = self.content[0]

    def filestat_command(self, id: int):
        try:
            descriptor = self.descriptors[id]
            print(f'Descriptor {id}: file_type:{descriptor.file_type}, links:{descriptor.links}, '
                  f'file_size:{descriptor.file_size}, memory_location:{descriptor.memory_location}')
        except IndexError:
            print(f'Descriptor id "{id}" doesn\'t exist')

    def ls_command(self):
        for elem in range(len(self.cur_dir.content)):
            for link in self.cur_dir.content[elem].links:
                print(f'File: "{link}", descriptor_id: {self.cur_dir.content[elem].descriptor_id}')

    def create_command(self, name):
        size = random.randint(1, 4)
        if self.bit_map.count(0) < size:
            print(f'[ERROR] Unable to create the file, not enough memory')
            return
        elif len(self.descriptors) == self.max_descriptors_num and None not in self.descriptors:
            print(f'[ERROR] Unable to create the file, exceeded the max number of descriptors')
            return

        cur_dir, elem = self.findpath(name)

        if not cur_dir:
            return
        elif not elem:
            print(f'[ERROR] name "{elem}" is forbidden')
            return

        for file in cur_dir.content:
                if name in file.links:
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
        if None in self.descriptors:
            desc_id = self.descriptors.index(None)
            self.descriptors[desc_id] = Descriptor('file', size, location)
            cur_dir.content.append(File(elem, id))
        else:
            self.descriptors.append(Descriptor('file', size, location))
            cur_dir.content.append(File(elem, len(self.descriptors) - 1))
        print(f'"{name}" has been created')

    def open_command(self, name):
        cur_dir, elem = self.findpath(name)

        if not cur_dir:
            return
        elif not elem:
            print(f'[ERROR] "{elem}" is not a file')
            return

        for file in cur_dir.content:
            if elem in file.links:
                if self.descriptors[file.descriptor_id].file_type == 'file':
                    r = random.randint(1000, 5000)
                    while r in self.fds:
                        r = random.randint(1000, 5000)
                    self.descriptors[file.descriptor_id].fd = r
                    self.fds.add(r)
                    print(f'File "{name}" has been opened, fd: "{self.descriptors[file.descriptor_id].fd}"')
                    return
                else:
                    print(f'[ERROR] "{name}" is not a file')
                    return
        else:
            print(f'[ERROR] File doesn\'t exist')

    def close_command(self, fd):
        for descriptor in self.descriptors:
            if descriptor.fd == fd:
                descriptor.fd = None
                print(f'File with fd:{fd} has been closed')
                return
        else:
            print(f'[ERROR] There is no opened file with such fd: "{fd}"')

    def read_command(self, fd, offset, size):
        for descriptor in self.descriptors:
            if descriptor.fd == fd:
                # check the offset
                if offset > len(descriptor.memory_location):
                    print(f'[ERROR] Can not read. The offset is too large')
                else:
                    print(' '.join(self.memory[descriptor.memory_location[0] + offset].block_value[:size]))
                    # print(' '.join(self.memory[descriptor.memory_location[offset]].block_value[:size]))
                return
        else:
            print(f'[ERROR] Can not read. There is no opened file with such fd: "{fd}"')

    def write_command(self, fd, offset, size):
        for descriptor in self.descriptors:
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
        else:
            print(f'[ERROR] Can not write. There is no opened file with such fd: "{fd}"')

    def link_command(self, name1: 'existing file', name2: 'new link name'):
        all_links = []
        for file in self.cur_dir.content:
            all_links.extend(file.links)
        
        if name1 not in all_links:
            print(f'[ERROR] File "{name1}" doesn\'t exist')
            return

        for file in self.cur_dir.content:
            for link in file.links:
                if link == name1:
                    if name2 in file.links:
                        print(f'[ERROR] The link "{name2}" already exists')
                        return
                    elif name2 in all_links:
                        print(f'[ERROR] This link "{name2}" is already busy')
                        return
                    elif self.descriptors[file.descriptor_id].file_type != 'file':
                        print(f'[ERROR] Can\'t create hard link. "{name2}" is not a file')
                        return
                    file.links.append(name2)
                    self.descriptors[file.descriptor_id].links += 1

    def unlink_command(self, name):
        for elem in range(len(self.cur_dir.content)):
            if name in self.cur_dir.content[elem].links:
                if self.descriptors[self.cur_dir.content[elem].descriptor_id].file_type != 'file':
                    print(f'[ERROR] Unable to delete hard link. "{name}" is nor a file')
                    return
                self.cur_dir.content[elem].links.remove(name)
                self.descriptors[self.cur_dir.content[elem].descriptor_id].links -= 1
                print(f'The link "{name}" was deleted')
                # check if there is only one link in links -> del the descriptor and free up the memory and bit_map
                if self.descriptors[self.cur_dir.content[elem].descriptor_id].links == 0:
                    for block_id in self.descriptors[self.cur_dir.content[elem].descriptor_id].memory_location:
                        self.memory[block_id].block_value = ['_'] * self.memory[0].block_size
                        self.bit_map[block_id] = 0
                    self.descriptors[self.cur_dir.content[elem].descriptor_id] = None
                    del self.cur_dir.content[elem]
                    print(f'Descriptor has been deleted. There was only one link for this file.')
                return
        else:
            print(f'[ERROR] There is no such link in FileSystem: "{name}"')

    def truncate_command(self, name, size: 'only [1, 2, 3, 4] values are valid here'):
        if size < 1 or size > 4:
            print(f'[ERROR] Inappropriate size value "{size}". '
                  f'In this FileSystem allowed only range from 1 to 4 blocks for the file size')
            return

        cur_dir, elem = self.findpath(name)

        if not cur_dir:
            return
        elif not elem:
            print(f'[ERROR] "{elem}" is not a file')
            return

        for file in cur_dir.content:
            if elem in file.links:
                if self.descriptors[file.descriptor_id].file_type == 'file':
                    # not to change if the given size is the same
                    if len(self.descriptors[file.descriptor_id].memory_location) == size:
                        print(f'Nothing has been changed. The given size is equal to the old one')
                        return

                    old_size = len(self.descriptors[file.descriptor_id].memory_location)

                    # to reduce the file
                    if size < old_size:
                        popped = []
                        popped.extend(self.descriptors[file.descriptor_id].memory_location[size:])
                        del self.descriptors[file.descriptor_id].memory_location[size:]
                        # free up the memory and bit_map
                        for i in popped:
                            self.memory[i].block_value = ['_'] * self.memory[0].block_size
                            self.bit_map[i] = 0
                        self.descriptors[file.descriptor_id].file_size = size

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
                                    self.descriptors[file.descriptor_id].memory_location.append(i)
                                if not s:
                                    break
                            self.descriptors[file.descriptor_id].file_size = size

                    print(f'File {name} took {old_size} block(s)')
                    print(f'Now file {name} takes {size} block(s)')
                else:
                    print(f'[ERROR] "{name}" is not a file')
        else:
            print(f'[ERROR] There is no such link in FileSystem: "{name}"')

    def mkdir_command(self, name):
        if (self.max_descriptors_num - len(self.descriptors) + self.descriptors.count(None)) < 3:
            print('[ERROR] Unable to create directory. Not enough descriptors')
            return
        elif not name.isalnum():
            print(f'[ERROR] "{name}" is forbidden')
            return
        else:
            for file in self.cur_dir.content:
                for link in file.links:
                    if link == name:
                        print(f'[ERROR] file "{name}" already exists')
                        return

        if None in self.descriptors:
            id = self.descriptors.index(None)
            self.descriptors[id] = Descriptor('directory', 0, [])
            self.cur_dir.content.append(Directory(name, id))
        else:
            self.descriptors.append(Descriptor('directory', 0, []))
            self.cur_dir.content.append(Directory(name, len(self.descriptors) - 1))

        path = self.cur_dir.content[1].path

        if None in self.descriptors:
            id = self.descriptors.index(None)
            self.descriptors[id] = Descriptor('symlink', 0, [])
            self.cur_dir.content[-1].content.append(Symlink('..', id, path))
        else:
            self.descriptors.append(Descriptor('symlink', 0, []))
            self.cur_dir.content[-1].content.append(Symlink('..', len(self.descriptors) - 1, path))

        if path[-1] == "/":
            path += f'{name}'
        else:
            path += f'/{name}'

        if None in self.descriptors:
            id = self.descriptors.index(None)
            self.descriptors[id] = Descriptor('symlink', 0, [])
            self.cur_dir.content[-1].content.append(Symlink('.', id, path))
        else:
            self.descriptors.append(Descriptor('symlink', 0, []))
            self.cur_dir.content[-1].content.append(Symlink('.', len(self.descriptors) - 1, path))

        print(f'Directory "{name}" has been created')

    def rmdir_command(self, name):
        for elem in range(len(self.cur_dir.content)):
            if name == self.cur_dir.content[elem].links[0]:
                id = self.cur_dir.content[elem].descriptor_id
                if self.descriptors[id].file_type != 'directory':
                    print(f'[ERROR] "{name}" is not a directory')
                    return
                for i in range(len(self.cur_dir.content[elem].content)):
                    id2 = self.cur_dir.content[elem].content[i].descriptor_id
                    if self.descriptors[id2].file_type != 'symlink':
                        print(f'[ERROR] Not an empty directory "{name}"')
                        return
                for j in range(len(self.cur_dir.content[elem].content)):
                    id2 = self.cur_dir.content[elem].content[j].descriptor_id
                    self.descriptors[id2] = None
                self.descriptors[id] = None
                del(self.cur_dir.content[elem])
                print(f'Directory "{name}" has been deleted')
                return
        else:
            print(f'[ERROR] There is no such directory in FileSystem')

    def symlink_command(self, path, name):
        all_links = []
        for file in self.cur_dir.content:
            all_links.extend(file.links)

        if not name.isalnum():
            print(f'[ERROR] name "{name}" is forbidden')
            return

        if name in all_links:
            print(f'[ERROR] link "{name}" is already used')
            return

        if len(self.descriptors) == self.max_descriptors_num and None not in self.descriptors:
            print(f'[ERROR] Unable to create the link, exceeded the max number of descriptors')
            return

        path_ = path.split("/")
        for i in range(len(path_)):
            if i == 0 and path_[i] == '':
                pass
            else:
                if not path_[i].isalnum():
                    print(f'[ERROR] unreachable path "{path}"')
                    return

        if None in self.descriptors:
            id = self.descriptors.index(None)
            self.descriptors[id] = Descriptor('symlink', 0, [])
            self.cur_dir.content.append(Symlink(name, id, path))
        else:
            self.descriptors.append(Descriptor('symlink', 0, []))
            self.cur_dir.content.append(Symlink(name, len(self.descriptors) - 1, path))
        print(f'Symlink "{name}" has been created')

    def cd_command(self, name):
        if name == '/':
            self.cur_dir = self.content[0]
            return

        cur_dir, elem = self.findpath(name)
        if not cur_dir:
            return
        elif not elem:
            self.cur_dir = cur_dir
            return

        for file in cur_dir.content:
            if elem == file.links[0]:
                id = file.descriptor_id
                if self.descriptors[id].file_type == 'directory':
                    self.cur_dir = file
                    return
                else:
                    print(f'[ERROR] "{name}" is not a directory')
                    return
        else:
            print(f'[ERROR] There is no such directory "{name}"')

    def findpath(self, path):
        split_path = path.split("/")
        for i in range(len(split_path)):
            if i == 0 and split_path[i] == '':
                pass
            else:
                if not split_path[i].isalnum() and split_path[i] != '..' and split_path[i] != '.':
                    print(f'[ERROR] unreachable path "{path}"')
                    return 0, 0

        if split_path[0] == '':
            cur_dir = self.content[0]
            split_path.remove('')
        else:
            cur_dir = self.cur_dir

        i = 0
        all_links = 0
        for part in split_path:
            i += 1
            flag1 = 1
            flag2 = 0
            for file in cur_dir.content:
                if part in file.links:
                    flag1 = 0
                    id = file.descriptor_id
                    if self.descriptors[id].file_type != 'symlink' and i == len(split_path):
                        return cur_dir, part
                    elif self.descriptors[id].file_type == 'file':
                        print(f'[ERROR] There is no such file or directory "{path}"')
                        return 0, 0
                    elif self.descriptors[id].file_type == 'directory':
                        cur_dir = file
                        break
                    else:
                        all_links += 1
                        if all_links == 3:
                            print(f'[ERROR] Exceeded amount of nested links')
                            return 0, 0
                        else:
                            if file.path == '/':
                                cur_dir = self.content[0]
                                flag2 = 1
                                break
                            nested_path = file.path.split("/")
                            if nested_path[0] == '':
                                cur_dir = self.content[0]
                                nested_path.remove('')
                            for j in range(len(nested_path)):
                                split_path.insert(i + j, nested_path[j])
                            break
            if flag1 and i == len(split_path):
                return cur_dir, part
            elif flag2 and i == len(split_path):
                return cur_dir, 0
            elif flag1 and i != len(split_path):
                print(f'[ERROR] There is no such file or directory "{path}"')
                return 0, 0


if __name__ == '__main__':
    mounted = False
    cur_dir = ''
    while True:
        user_input = input(cur_dir + '$ ').split()
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

        # filestat create open close unlink mkdir rmdir cd
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
            elif user_input[0] == 'mkdir':
                try:
                    fs.mkdir_command(user_input[1])
                except NameError:
                    pass
            elif user_input[0] == 'rmdir':
                try:
                    fs.rmdir_command(user_input[1])
                except NameError:
                    pass
            elif user_input[0] == 'cd':
                try:
                    fs.cd_command(user_input[1])
                except NameError:
                    pass
            elif (user_input[0] == 'link' or user_input[0] == 'truncate' or user_input[0] == 'read'
                  or user_input[0] == 'write') and mounted:
                print(f'Not enough arguments')

        # link truncate symlink
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
            elif user_input[0] == 'symlink':
                try:
                    fs.symlink_command(user_input[1], user_input[2])
                except NameError:
                    pass
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
        if mounted:
            cur_dir = fs.cur_dir.content[1].path
        else:
            cur_dir = ''
