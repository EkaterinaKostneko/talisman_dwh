from xml.sax import handler, parse

from ..reader import AbstractReader, AbstractSheet


class ExcelHandler(handler.ContentHandler):

    def __init__(self):
        self.chars = []
        self.cells = []
        self.rows = []
        self.tables = []
        self.attrs = None
        self.row_styled = False

    def characters(self, content):
        self.chars.append(content)

    def startElement(self, name, atts):
        if name == "Cell":
            self.attrs = atts._attrs
            self.chars = []
        elif name == "NamedCell":
            if self.row_styled:
                self.cells.append(''.join(self.chars))
        elif name == "Row":
            self.cells = []
            attrs = atts._attrs
            self.row_styled = len(attrs) and 'ss:StyleID' in attrs
        elif name == "Table":
            self.rows = []

    def endElement(self, name):
        if name == "Cell":
            if not len(self.attrs):
                self.chars = []
            else:
                if not self.row_styled:
                    self.cells.append(''.join(self.chars))
        elif name == "Row":
            self.rows.append(self.cells)
        elif name == "Table":
            self.tables.append(self.rows)


class Excel2003XMLSheet(AbstractSheet):
    """ "Лист" данных из Excel 2003 XML Spreadsheet файла """

    def to_array(self):
        return self.data.tables[0]


class Reader(AbstractReader):
    """ Ридер Excel 2003 XML Spreadsheet файлов """

    @property
    def sheets(self):
        if not self._sheets:
            self._sheets = {1: self.stream[0]}

        return self._sheets

    def get_stream(self, path, **kwargs):
        excel_handler = ExcelHandler()
        parse(path, excel_handler)

        sheet = Excel2003XMLSheet(excel_handler)

        self.stream = [sheet]

        return self.stream

    def get_rows(self, **kwargs):
        sheet = kwargs.get('sheet', 0)
        self.sheet_stream = self.stream

        return self.sheet_stream[sheet].to_array()
