import os, sys, time, re
import importlib, pkgutil
import argparse
import socket, json
import tempfile, subprocess, traceback
from cmd import Cmd
from operator import itemgetter
from datetime import datetime

from arcus_util import zookeeper


class ResultPrinter:
    def __init__(self):
        self.out = sys.stdout

        self.usage_filter = '''
    # USAGE: -f
        print only selected attributes
        selected attributes are comma seperated input
            ex) -f Id pgs-info * : print only Id from pgs-info result
            ex) -f Id,PgId pgs-info * : print only Id and PgId from pgs-info result
    '''

        self.usage_sort = '''
    # USAGE: -s
        sort result by attrute
        selected attributes are comma seperated
        default is ascending order. + ends means ascending, - ends means descending
            ex) -s Id pgs-info * : sort with Id (ascending order) of pgs-info result
            ex) -s Id- pgs-info * : sort with Id (descending order) of pgs-info result
            ex) -s PgId,Id+ pgs-info * : sort with PgId and pgs Id (ascending order) of pgs-info result
    '''

        self.usage_json = '''
    # USAGE: -j
        print result with json format
            ex) -j pgs-info * : print with json format of pgs-info
    '''

        self.usage_grep = '''
    # USAGE: -g
        filter the results with grep values
            ex) -j adevimc.* pgs-info * : print pgs-info results which match "adevimc.*" in it's value
    '''

        self.usage_edit = '''
    # USAGE: -e
        edit command result
        you can use printed result with grep or other tools after save
            ex) -e -b pmr-info .* : edit pmr-info result with vim. 
    '''

    def print_usage(self):
        print(self.usage_filter)
        print(self.usage_sort)
        print(self.usage_json)
        print(self.usage_grep)
        print(self.usage_edit)
        print(self.get_brief_option.__doc__)

    def __json_filter(self, result, opt):
        filter_list = opt['filter'].split(',')
        new_result = []
        for item in result:
            if isinstance(item, dict):
                obj = item
            else:
                obj = json.loads(item)

            new_obj = {}
            for filter in filter_list:
                new_obj[filter] = obj[filter]
            new_result.append(json.dumps(new_obj))

        return new_result

    def __json_sort(self, result, opt):
        key = opt['sort']
        is_reverse = False
        if key[-1] == '-':
            key = key[:-1]
            is_reverse = True
        elif key[-1] == '+':
            key = key[:-1]

        key_list = key.split(',')
        new_result = []
        for item in result:
            if isinstance(item, dict):
                obj = item
            else:
                obj = json.loads(item)

            new_obj = {}
            new_result.append(obj)

        result =  sorted(new_result, key=itemgetter(*key_list), reverse=is_reverse)

        new_result = []
        for item in result:
            new_result.append(json.dumps(item))

        return new_result

    def __json_loader(self, result, opt, as_string = False):
        # filter selected attrs
        if 'filter' in opt:
            result = self.__json_filter(result, opt)

        # sort
        if 'sort' in opt:
            result = self.__json_sort(result, opt)

        # convert as json
        new_result = []
        for item in result:
            if isinstance(item, dict):
                obj = item
            else:
                obj = json.loads(item)

            if as_string:
                new_result.append(json.dumps(obj, sort_keys=True, indent=4))
            else:
                new_obj = {}
                for k, v in list(obj.items()):
                    new_obj[k] = str(v)
                new_result.append(new_obj)

        return new_result

    def __print_as_json(self, result, opt):
        try:
            new_result = self.__json_loader(result, opt, True)
            if len(new_result) > 1:
                print('# total %d item' % len(new_result))
            idx = 0
            for item in new_result:
                if len(new_result) > 1:
                    print('## item %d -----------------------------' % idx)
                print(item)
                idx += 1
        except Exception as e: # not a json type, print text itself
            print(result)

    def pmr_slice_b1_callback(self, pmr_list):
        ret = []
        for pmr in pmr_list:
            pmr = json.loads(pmr)
            slices = pmr['PMSlices']

            values = list(slices.values())
            values = sorted(values, key = lambda v:(v['Cluster'], v['Type'], v['Id']))

            result = []
            for v in values:
                result.append('%-30s %5s %4s (%5d)' % (v['Cluster'], v['Type'], v['Id'], v['BasePort']))

            pmr['PMSlices'] = '\n\t' + '\n\t'.join(result)
            ret.append(json.dumps(pmr))

        return ret

    def pmr_slice_b2_callback(self, pmr_list):
        ret = []
        for pmr in pmr_list:
            pmr = json.loads(pmr)
            slices = pmr['PMSlices']

            values = list(slices.values())
            values = sorted(values, key = lambda v:(v['Cluster'], v['Type'], v['Id']))

            result = []
            prev_cluster = None
            count = 0
            for v in values:
                if prev_cluster == v['Cluster']:
                    count += 1
                    continue

                if prev_cluster != v['Cluster'] and prev_cluster != None:
                    result.append('%-30s %4d' % (prev_cluster, count))
                    count = 0
                prev_cluster = v['Cluster']

            # last one
            if prev_cluster != None:
                result.append('%-30s %4d' % (prev_cluster, count))

            pmr['PMSlices'] = '\n\t' + '\n\t'.join(result)
            ret.append(json.dumps(pmr))

        return ret

    def __print_as_table(self, result, opt):
        try:
            new_result = self.__json_loader(result, opt, False)
            if len(new_result) == 0:
                'ERROR: nothing to print'
                return

            # calculate max len of attributes
            maxlen_list = []
            if 'filter' in opt:
                attrs = opt['filter'].split(',')
            else:
                attrs = list(new_result[0].keys())

            for i in range(len(attrs)):
                maxlen_list.append(len(attrs[i]))

            for item in new_result:
                values = []
                for attr in attrs:
                    values.append(item[attr])
                for i in range(len(attrs)):
                    if len(values[i]) > maxlen_list[i]:
                        maxlen_list[i] = len(values[i])

                    # ignore too long attribute indent
                    if maxlen_list[i] > 16:
                        maxlen_list[i] = 16

            # print header
            for i in range(len(attrs)):
                attr = attrs[i]
                margin = maxlen_list[i] - len(attr) + 4
                self.out.write(attr)
                self.out.write(' ' * margin)
            self.out.write('\n')

            # print -----------------
            for max in maxlen_list:
                self.out.write('-' * (max + 4))
            self.out.write('\n')

            # print values
            for item in new_result:
                values = []
                for attr in attrs:
                    values.append(item[attr])

                line = ''
                for i in range(len(attrs)):
                    value = values[i]
                    margin = maxlen_list[i] - len(value) + 4
                    line += value
                    line += ' ' * margin

                matched = True
                if 'grep' in opt:
                    matched = False
                    for v in values:
                        if re.search(opt['grep'], str(v)) is not None:
                            matched = True
                            break

                if matched:
                    self.out.write(line + '\n')

        except Exception as e: # not a json type, print text itself
            print(result)

    def write(self, result, opt):
        if 'callback' in opt:
            result = opt['callback'](result)

        if 'edit' in opt:
            with tempfile.NamedTemporaryFile(suffix=".tmp") as tf:
                try:
                    out_back = self.out
                    self.out = tf

                    if 'json' in opt:
                        self.__print_as_json(result, opt)
                    else: # table
                        self.__print_as_table(result, opt)

                    tf.flush()
                    editor = os.environ.get('EDITOR', 'vim')
                    subprocess.call([editor, tf.name])

                finally:
                    self.out = out_back

        else:
            if 'json' in opt:
                self.__print_as_json(result, opt)
            else: # table
                self.__print_as_table(result, opt)



class CommandHandler(Cmd):
    def __init__(self, zk, cloud):
        Cmd.__init__(self)
        self.zk = zk
        self.cloud = cloud
        self.printer = ResultPrinter()
        self.reset()
        self.zk_cmd = {
            'ls': self._do_zk_ls,
        }

        self.cloud_cmd = {
            'ls': self._do_ls,
        }

    def reset(self):
        self.prompt = '%s(%s)> ' % (self.zk.address, self.cloud)

    def clierr(self, s):
        print("*** %s" % s)

    def _do_zk_ls(self, cmd, opt):
        ret = []
        for k, v in self.zk.arcus_cache_map.items():
            ret.append({'name':k})

        return ret

    def _do_ls(self, cmd, opt):
        ret = []
        for node in self.zk.arcus_cache_map[self.cloud].node:
            ret.append({'cloud':node.code, 'addr':'%s:%s' % (node.ip, node.port), 'acctive':node.active})

        return ret

    def do_select(self, cloud_name):
        self.cloud = cloud_name
        self.reset()

    def do_help(self, about):
        about = about.strip()

        if about == 'option':
            self.printer.print_usage()

        else:
            print('''
    HELP categories
        help : this itself
        help option : about result printer option (-b, -f etc)
    ''')


    def do_quit(self, s):
        return True

    def process_cmd(self, cmd, opt):
        if self.cloud == None:
            if opt['toks'][0] in self.zk_cmd:
                ret = self.zk_cmd[opt['toks'][0]](cmd, opt)
            else:
                self.clierr('unknown command: %s' % opt['toks'][0])
                return
        else:
            if opt['toks'][0] in self.cloud_cmd:
                ret = self.cloud_cmd[opt['toks'][0]](cmd, opt)
            else:
                self.clierr('unknown command: %s' % opt['toks'][0])
                return

        self.printer.write(ret, opt)

    def emptyline(self):
        return

    def default(self, line):
        return

    def get_option(self, line):
        toks = line.split()
        opt = {'input':line}
        cmd_toks = []
        cmd_line = ''

        i = 0
        while i < len(toks):
            tok = toks[i]

            if tok.startswith('-b'):
                opt['buffer'] = True
            elif tok == '-j':
                opt['json'] = True
            elif tok == '-f':       # -f Id,PgId,Role,Color
                if len(toks) <= i+1:
                    print(self.printer.usage_filter)
                    return opt, []

                opt['filter'] = toks[i+1]
                i += 1
            elif tok == '-s':       # -s PgId,Id+
                if len(toks) <= i+1:
                    print(self.printer.usage_sort)
                    return opt, []

                opt['sort'] = toks[i+1]
                i += 1
            elif tok == '-g':       # -g localhost (egrep)
                if len(toks) <= i+1:
                    print(self.printer.usage_grep)
                    return opt, []

                opt['grep'] = toks[i+1]
                i += 1
            elif tok == '-e':       # -e (edit result)
                opt['edit'] = True
            else: # end of prefix option
                cmd_toks = toks[i:]
                tmp = line.split(None, i)
                cmd_line = tmp[-1]
                break

            i+=1

        if len(cmd_toks) == 0:
            if 'filter' in opt:
                print(self.printer.usage_filter)

            if 'sort' in opt:
                print(self.printer.usage_sort)

            if 'grep' in opt:
                print(self.printer.usage_grep)

            if 'json' in opt:
                print(self.printer.usage_json)

            if 'edit' in opt:
                print(self.printer.usage_edit)

        opt['toks'] = cmd_toks
        return opt, cmd_line

    def do_cmd(self, line):
        opt, cmd_line = self.get_option(line)
        if cmd_line.strip() == '':
            return

        self.process_cmd(cmd_line, opt)

    def precmd(self, line):
        if line is None or line.strip() == '':
            return ''

        if line.startswith(':'):
            return line[1:]

        if line.strip() == 'quit':
            return line

        return 'cmd ' + line

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    sp = ap.add_subparsers(dest='mode')
    ap.add_argument("-zk", dest="zookeeper_address", type=str, default='', required=False,
                    help='address of zookeeper, DNS or comma separated address list')
    ap.add_argument("-c", dest="cloud", type=str, help='arcus cloud name')

    cmd = sp.add_parser('cmd')
    cmd.set_defaults(mode = 'cmd')

    args = ap.parse_args()

    if args.zookeeper_address == '':
        print('zookeeper address is invalid or absent')
        sys.exit(-1)

    zk = zookeeper(args.zookeeper_address)
    zk.load_all()

    if args.mode == 'cmd':
        CommandHandler(zk, args.cloud).cmdloop()
    else:
        print('unknown sub command')