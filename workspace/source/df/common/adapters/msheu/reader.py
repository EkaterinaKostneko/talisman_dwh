from xml.etree import ElementTree as ET

from ..reader import AbstractReader, AbstractSheet


class MSHEUSheet(AbstractSheet):
    """ "Лист" данных из файла электронного учета МинСельХоза """

    SPLITTER = 'SPLITTER'

    def parse(self, node, res, attrs, index=1):
        node_res = []
        for child in node:
            child_attrs = {**attrs, **{f'{node.tag}{self.SPLITTER}{key}': value for key, value in node.attrib.items()}}
            if len(child):
                res, index = self.parse(child, res, child_attrs, index)
            elif child.text:
                node_res += [{**child_attrs, f"element{self.SPLITTER}name": node.tag, f"element{self.SPLITTER}index": index,
                              f"parameter{self.SPLITTER}tag": child.tag, f"parameter{self.SPLITTER}value": child.text}]
        index = 1 if not node_res else index + 1
        return res + node_res, index

    def to_array(self):
        array = list()
        if len(self.data):
            parsed_list = []
            for node in self.data:
                res, index = self.parse(node, list(), dict())
                parsed_list += res
            splitted_header = [h.split(self.SPLITTER) for h in parsed_list[0].keys()]
            array += list(zip(*splitted_header))
            array += [list(v.values()) for v in parsed_list]
        return array


class Reader(AbstractReader):
    """ Ридер MSHEU файлов """

    @property
    def sheets(self):
        if not self._sheets:
            self._sheets = {1: self.stream[0]}

        return self._sheets

    def get_stream(self, path, **kwargs):
        tree = ET.parse(path)
        root = tree.getroot()

        sheet = MSHEUSheet(root)

        self.stream = [sheet]

        return self.stream

    def get_rows(self, **kwargs):
        sheet = kwargs.get('sheet', 0)
        self.sheet_stream = self.stream

        return self.sheet_stream[sheet].to_array()
