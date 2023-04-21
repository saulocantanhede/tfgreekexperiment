import sys
import collections
import re
from io import BytesIO

from lxml import etree
from ..fabric import Fabric
from ..core.helpers import console
from ..convert.walker import CV
from ..core.files import (
    abspath,
    expanduser as ex,
    unexpanduser as ux,
    getLocation,
    initTree,
    dirNm,
    dirExists,
    scanDir,
)


__pdoc__ = {}

DOC_TRANS = """
## Essentials

*   Text-Fabric non-slot nodes correspond to XML elements in the source.
*   Text-Fabric node-features correspond to XML attributes.
*   Text-Fabric slot nodes correspond to characters in XML element content.

## Sectioning

The material is divided into two levels of sections, mainly for the purposes
of text display.

It is assumed that the source is a directory consisting of subdirectories
consisting of xml files, the XML files.

1.  Subdirectories and files are sorted in the lexicographic ordering
1.  The subdirectory `__ignore__` is ignored.
1.  For each subdirectory, a section level 1 node will be created, with
    feature `name` containing its name.
1.  For each file in a subdirecotry, a section level 2 node will be created, with
    feature `name` containing its name.


## Elements and attributes

1.  All elements result in nodes whose type is
    exactly equal to the tag name.
1.  These nodes are linked to the slots that are produced when converting the
    content of the corresponding source elements.
1.  Attributes translate into features of the same name; the feature assigns
    the attribute value (as string) to the node that corresponds to the element
    of the attribute.

## Slots

The basic unit is the unicode character.
For each character in the input we make a slot, but the correspondence is not
quite 1-1.

1.  Whitespace is reduced to a single space.
1.  Empty elements will receive one extra slot; this will anchor the element to
    a textual position; the empty slot gets the ZERO-WIDTH-SPACE (Unicode 200B)
    as character value.
1.  Slots get the following features:
    *   `ch`: the character of the slot
    *   `empty`: 1 if the slot has been inserted as an empty slot, no value otherwise.


## Text-formats

Text-formats regulate how text is displayed, and they can also determine
what text is displayed.

We have the following formats:

*   `text-orig-full`: all text

## Simplifications

XML is complicated.

On the other hand, the resulting TF should consist of clearly demarcated node types
and a simple list of features. In order to make that happen, we simplify matters
a bit.

1.  Processing instructions (`<!proc a="b">`) are ignored.
1.  Comments (`<!-- this is a comment -->`) are ignored.
1.  Declarations (`<?xml ...>` `<?xml-model ...>` `<?xml-stylesheet ...>`) are
    read by the parser, but do not leave traces in the TF output.
1.  The atrributes of the root-element are ignored.
1.  Namespaces are read by the parser,
    but only the unqualified names are distinguishable in the output as feature names.
    So if the input has elements `ns1:abb` and `ns2:abb`, we'll see just the node
    type `abb` in the output.

## TF noded and features

(only in as far they are not in 1-1 correspondence with XML elements and attributes)

### node type `folder`

*The type of subfolders of XML documents.*

**Section level 1.**

**Features**

feature | description
--- | ---
`folder` | name of the subfolder

### node type `file`

*The type of individual XML documents.*

**Section level 2.**

**Features**

feature | description
--- | ---
`file` | name of the file, without the `.xml` extension. Other extensions are included.

### node type `char`

*Unicode characters.*

**Slot type.**

The characters of the text of the elements.
Ignorable whitespace has been discarded, and is not present in the TF dataset.
Meaningful whitespace has been condensed to single spaces.

Some empty slots have been inserted to mark the place of empty elements.

**Features**

feature | description
--- | ---
`ch` | the unicode character in that slot. There are also slots
`empty` | whether a slot has been inserted in an empty element
"""


class XML:
    def __init__(
        self,
        sourceVersion="0.1",
        testSet=set(),
        generic={},
        transform=None,
        tfVersion="0.1",
    ):
        (backend, org, repo, relative) = getLocation()
        if any(s is None for s in (backend, org, repo, relative)):
            console(
                "Not working in a repo: "
                f"backend={backend} org={org} repo={repo} relative={relative}"
            )
            quit()

        console(f"Working in repository {org}/{repo}{relative} in backend {backend}")

        base = ex(f"~/{backend}")
        repoDir = f"{base}/{org}/{repo}"
        refDir = f"{repoDir}{relative}"
        sourceDir = f"{refDir}/xml/{sourceVersion}"
        reportDir = f"{refDir}/report"
        tfDir = f"{refDir}/tf"

        self.refDir = refDir
        self.sourceDir = sourceDir
        self.reportDir = reportDir
        self.tfDir = tfDir
        self.org = org
        self.repo = repo

        if sourceDir is None or not dirExists(sourceDir):
            console(f"Source location does not exist: {sourceDir}")
            quit()

        self.sourceVersion = sourceVersion
        self.testMode = False
        self.testSet = testSet
        self.generic = generic
        self.transform = transform
        self.tfVersion = tfVersion
        self.tfPath = f"{tfDir}/{tfVersion}"
        myDir = dirNm(abspath(__file__))
        self.myDir = myDir

    @staticmethod
    def help(program):

        console(
            f"""

        Convert XML to TF.
        There are also commands to check the XML and to load the TF.

        python3 {program} [tasks/flags] [--help]

        --help: show this text and exit

        tasks:
            a sequence of tasks:
            check:
                just reports on the elements in the source.
            convert:
                just converts XML to TF
            load:
                just loads the generated TF;

        flags:
            test:
                run in test mode
        """
        )

    @staticmethod
    def getParser():
        return etree.XMLParser(
            remove_blank_text=False,
            collect_ids=False,
            remove_comments=True,
            remove_pis=True,
            huge_tree=True,
        )

    def getXML(self):
        sourceDir = self.sourceDir
        testMode = self.testMode
        testSet = self.testSet

        IGNORE = "__ignore__"

        xmlFilesRaw = collections.defaultdict(list)

        with scanDir(sourceDir) as dh:
            for folder in dh:
                folderName = folder.name
                if folderName == IGNORE:
                    continue
                if not folder.is_dir():
                    continue
                with scanDir(f"{sourceDir}/{folderName}") as fh:
                    for file in fh:
                        fileName = file.name
                        if not (fileName.lower().endswith(".xml") and file.is_file()):
                            continue
                        if testMode and fileName not in testSet:
                            continue
                        xmlFilesRaw[folderName].append(fileName)

        xmlFiles = tuple(
            (folderName, tuple(sorted(fileNames)))
            for (folderName, fileNames) in sorted(xmlFilesRaw.items())
        )
        return xmlFiles

    def checkTask(self):
        sourceDir = self.sourceDir
        reportDir = self.reportDir

        getStore = lambda: collections.defaultdict(  # noqa: E731
            lambda: collections.defaultdict(collections.Counter)
        )
        analysis = getStore()

        parser = self.getParser()

        initTree(reportDir)

        def analyse(root, analysis):
            NUM_RE = re.compile(r"""[0-9]""", re.S)

            def nodeInfo(node):
                tag = etree.QName(node.tag).localname
                atts = node.attrib

                if len(atts) == 0:
                    analysis[tag][""][""] += 1
                else:
                    for (kOrig, v) in atts.items():
                        k = etree.QName(kOrig).localname

                        vTrim = NUM_RE.sub("N", v)
                        analysis[tag][k][vTrim] += 1

                for child in node.iterchildren(tag=etree.Element):
                    nodeInfo(child)

            nodeInfo(root)

        def writeReport():
            reportFile = f"{reportDir}/elements.txt"
            with open(reportFile, "w", encoding="utf-8") as fh:
                fh.write(
                    "Inventory of tags and attributes in the source XML file(s).\n"
                )
                fh.write("\n\n")

                infoLines = 0

                def writeAttInfo(tag, att, attInfo):
                    nonlocal infoLines
                    nl = "" if tag == "" else "\n"
                    tagRep = "" if tag == "" else f"<{tag}>"
                    attRep = "" if att == "" else f"{att}="
                    atts = sorted(attInfo.items())
                    (val, amount) = atts[0]
                    fh.write(f"{nl}\t{tagRep:<12} {attRep:<12} {amount:>5}x {val}\n")
                    infoLines += 1
                    for (val, amount) in atts[1:]:
                        fh.write(f"""\t{'':<12} {'"':<12} {amount:>5}x {val}\n""")
                        infoLines += 1

                def writeTagInfo(tag, tagInfo):
                    nonlocal infoLines
                    tags = sorted(tagInfo.items())
                    (att, attInfo) = tags[0]
                    writeAttInfo(tag, att, attInfo)
                    infoLines += 1
                    for (att, attInfo) in tags[1:]:
                        writeAttInfo("", att, attInfo)

                for (tag, tagInfo) in sorted(analysis.items()):
                    writeTagInfo(tag, tagInfo)

            console(f"{infoLines} info line(s) written to {reportFile}")

        i = 0
        for (xmlFolder, xmlFiles) in self.getXML():
            console(xmlFolder)
            for xmlFile in xmlFiles:
                i += 1
                console(f"\r{i:>4} {xmlFile:<50}", newline=False)
                xmlPath = f"{sourceDir}/{xmlFolder}/{xmlFile}"
                tree = etree.parse(xmlPath, parser)
                root = tree.getroot()
                analyse(root, analysis)

        console("")
        writeReport()

        return True

    # SET UP CONVERSION

    def getConverter(self):
        tfPath = self.tfPath

        TF = Fabric(locations=tfPath)
        return CV(TF)

    def convertTask(self):
        slotType = 'word'
        otext = {
            'fmt:text-orig-full':'{word}',
            'sectionTypes':'book,chapter,verse',
            'sectionFeatures':'book,chapter,verse',
        }
        intFeatures = {
        'chapter',
        'verse',
        'book_num',
        'sentence_number',
        'nodeId',
        'strong',
        'word_in_verse',
        'empty',
        }
        featureMeta = dict(
            book_num=dict(description="NT book number (Matthew=1, Mark=2, ..., Revelation=27)"),
            book_short=dict(description="Book name (abbreviated)"),
            sentence_number=dict(description="Sentence number (counted per chapter)"),
            Rule=dict(description="Clause rule"),
            appositioncontainer=dict(description="Apposition container"),
            articular=dict(description="Articular"),
            class_wg=dict(description="Syntactical class"),
            clauseType=dict(description="Type of clause"),
            cltype=dict(description="Type of clause"),
            junction=dict(description="Type of junction"),
            nodeId=dict(description="Node ID (as in the XML source data"),
            role_wg=dict(description="Role"),
            rule=dict(description="Syntactical rule"),
            type_wg=dict(description="Syntactical type"),
            after=dict(description="After the end of the word"),
            book=dict(description="Book name (abbreviated)"),
            case=dict(description="Type of case"),
            chapter=dict(description="Number of the chapter"),
            class_w=dict(description="Morphological class"),
            degree=dict(description="Degree"),
            discontinuous=dict(description="Discontinuous"),
            domain=dict(description="domain"),
            frame=dict(description="frame"),
            gender=dict(description="gender"),
            gloss=dict(description="gloss"),
            id=dict(description="xml iD"),
            lemma=dict(description="lemma"),
            ln=dict(description="ln"),
            mood=dict(description="verbal mood"),
            morph=dict(description="morph"),
            normalized=dict(description="lemma normalized"),
            number=dict(description="number"),
            person=dict(description="person"),
            ref=dict(description="biblical reference with word counting"),
            referent=dict(description="number of referent"),
            role_w=dict(description="role"),
            strong=dict(description="strong number"),
            subjref=dict(description="number"),
            tense=dict(description="Verbal tense"),
            type_w=dict(description="Morphological type"),
            unicode=dict(description="lemma in unicode characters"),
            verse=dict(description="verse"),
            voice=dict(description="Verbal voice"),
            word_in_verse=dict(description="number of word"),
            empty=dict(description="whether a slot has been inserted in an empty element")
        )
        self.intFeatures = intFeatures
        self.featureMeta = featureMeta

        tfVersion = self.tfVersion
        tfPath = self.tfPath
        generic = self.generic
        generic["sourceFormat"] = "XML"
        generic["version"] = tfVersion

        initTree(tfPath, fresh=True, gentle=True)

        cv = self.getConverter()

        return cv.walk(
            self.getDirector(),
            slotType,
            otext=otext,
            generic=generic,
            intFeatures=intFeatures,
            featureMeta=featureMeta,
            generateTf=True
        )

    # DIRECTOR

    def getDirector(self):
        PASS_THROUGH = set(
            """
            xml
            """.strip().split()
        )

        # CHECKING

        ZWSP = "\u200b"  # zero-width space

        sourceDir = self.sourceDir
        featureMeta = self.featureMeta
        transform = self.transform

        transformFunc = (
            (lambda x: x)
            if transform is None
            else (lambda x: BytesIO(transform(x).encode("utf-8")))
        )

        parser = self.getParser()

        # WALKERS

        WHITE_TRIM_RE = re.compile(r"\s+", re.S)

        def walkNode(cv, cur, node):
            tag = etree.QName(node.tag).localname
            cur["nest"].append(tag)

            beforeChildren(cv, cur, node, tag)

            for child in node.iterchildren(tag=etree.Element):
                walkNode(cv, cur, child)

            afterChildren(cv, cur, node, tag)
            cur["nest"].pop()
            afterTag(cv, cur, node, tag)

        def addSlot(cv, cur, word):
            this_word = cv.slot()
            cv.feature(this_word, word=word)

        def beforeChildren(cv, cur, node, tag):
            if tag not in PASS_THROUGH:
                curNode = cv.node(tag)
                cur["elems"].append(curNode)
                atts = {etree.QName(k).localname: v for (k, v) in node.attrib.items()}
                if len(atts):
                    cv.feature(curNode, **atts)

            if node.text:
                textMaterial = WHITE_TRIM_RE.sub(" ", node.text)
                for word in textMaterial:
                    addSlot(cv, cur, word)

        def afterChildren(cv, cur, node, tag):
            if tag not in PASS_THROUGH:
                curNode = cur["elems"].pop()

                if not cv.linked(curNode):
                    s = cv.slot()
                    cv.feature(s, word=ZWSP, empty=1)

                cv.terminate(curNode)

        def afterTag(cv, cur, node, tag):
            if node.tail:
                tailMaterial = WHITE_TRIM_RE.sub(" ", node.tail)
                for word in tailMaterial:
                    addSlot(cv, cur, word)

        def director(cv):
            cur = {}

            i = 0
            for (xmlFolder, xmlFiles) in self.getXML():
                console(xmlFolder)

                cur["folder"] = cv.node("folder")
                cv.feature(cur["folder"], folder=xmlFolder)

                for xmlFile in xmlFiles:
                    i += 1
                    console(f"\r{i:>4} {xmlFile:<50}", newline=False)

                    cur["file"] = cv.node("file")
                    cv.feature(cur["file"], file=xmlFile.removesuffix(".xml"))

                    with open(f"{sourceDir}/{xmlFolder}/{xmlFile}", encoding="utf-8") as fh:
                        text = fh.read()
                        text = transformFunc(text)
                        tree = etree.parse(text, parser)
                        root = tree.getroot()
                        cur["nest"] = []
                        cur["elems"] = []
                        walkNode(cv, cur, root)

                    addSlot(cv, cur, None)
                    cv.terminate(cur["file"])

                cv.terminate(cur["folder"])

            console("")

            for fName in featureMeta:
                if not cv.occurs(fName):
                    cv.meta(fName)
            for fName in cv.features():
                if fName not in featureMeta:
                    cv.meta(
                        fName,
                        description=f"this is XML attribute {fName}",
                        valueType="str",
                    )
            console("source reading done")
            return True

        return director

    def loadTask(self):
        tfPath = self.tfPath

        if not dirExists(tfPath):
            console(f"Directory {ux(tfPath)} does not exist.")
            console("No tf found, nothing to load")
            return False

        TF = Fabric(locations=[tfPath])
        allFeatures = TF.explore(silent=True, show=True)
        loadableFeatures = allFeatures["nodes"] + allFeatures["edges"]
        api = TF.load(loadableFeatures, silent=False)
        if api:
            console(f"max node = {api.F.otype.maxNode}")
            return True
        return False

    def task(self, check=False, convert=False, load=False, test=None):
        sourceDir = self.sourceDir
        reportDir = self.reportDir
        tfPath = self.tfPath

        if test is not None:
            self.testMode = test

        good = True

        if check:
            console(f"XML to TF checking: {ux(sourceDir)} => {ux(reportDir)}")
            good = self.checkTask()

        if good and convert:
            console(f"XML to TF converting: {ux(sourceDir)} => {ux(tfPath)}")
            good = self.convertTask()

        if good and load:
            good = self.loadTask()

        return good

    def run(self, program=None):
        programRep = "XML-converter" if program is None else program
        possibleTasks = {"check", "convert", "load"}
        possibleFlags = {"test"}
        possibleArgs = possibleTasks | possibleFlags

        args = sys.argv[1:]

        if not len(args):
            self.help(programRep)
            console("No task specified")
            sys.exit(-1)

        illegalArgs = {arg for arg in args if arg not in possibleArgs}

        if len(illegalArgs):
            self.help(programRep)
            for arg in illegalArgs:
                console(f"Illegal argument `{arg}`")
            sys.exit(-1)

        tasks = {arg: True for arg in args if arg in possibleTasks}
        flags = {arg: True for arg in args if arg in possibleFlags}

        good = self.task(**tasks, **flags)
        if good:
            sys.exit(0)
        else:
            sys.exit(1)


__pdoc__["XML"] = DOC_TRANS