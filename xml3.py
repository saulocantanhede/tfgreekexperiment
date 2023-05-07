#importing libraries
import sys
import collections
import re
from io import BytesIO

from lxml import etree #lxml library is used to work with XML files

#importing Text-Fabric libraries
#these libraries are in the folder: ~/anaconda3/Lib/site-packages/tf
#and this file is in the folder: ~/anaconda3/Lib/site-packages/tf/convert
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
*   Text-Fabric slot nodes correspond to the words, which are the attribute unicode in XML element content.

## Source

It is assumed that the source is a directory consisting of XML files.

The files were preprocessed using a script which
indicates the sections used for locate each word (verse and chapter).

## Elements and attributes

1.  All elements result in nodes whose type is exactly equal to the tag name.
2.  These nodes are linked to the slots that are produced when converting the
    content of the corresponding source elements.
3.  Attributes translate into features of the same name; the feature assigns
    the attribute value (as string) to the node that corresponds to the element
    of the attribute.

## Slots

The basic unit is the word.

For each word in the input we make a slot, but the correspondence is not
quite 1-1.

1.  Whitespace is reduced to a single space.
2.  Empty elements will receive one extra slot; this will anchor the element to
    a textual position.
3.  Slots get the `w` feature as the word of the slot.

## Text-formats

Text-formats regulate how text is displayed, and they can also determine
what text is displayed.

We have the following formats:

*   `text-orig-full`: `{word}{after}`

## Simplifications

XML is complicated.

On the other hand, the resulting TF should consist of clearly demarcated node types
and a simple list of features. In order to make that happen, we simplify matters
a bit.

1.  Processing instructions (`<!proc a="b">`) are ignored.
2.  Comments (`<!-- this is a comment -->`) are ignored.
3.  Declarations (`<?xml ...>` `<?xml-model ...>` `<?xml-stylesheet ...>`) are
    read by the parser, but do not leave traces in the TF output.
4.  The atrributes of the root-element are ignored.
5.  Namespaces are read by the parser,
    but only the unqualified names are distinguishable in the output as feature names.
    So if the input has elements `ns1:abb` and `ns2:abb`, we'll see just the node
    type `abb` in the output.

## TF noded and features

(only in as far they are not in 1-1 correspondence with XML elements and attributes)

### node type `file`

*The type of individual XML documents.*

**Features**

feature | description
--- | ---
`file` | name of the file, without the `.xml` extension. Other extensions are included.

### node type `w`

*Words of the text.*

**Slot type.**

The words of the text of the elements.
Ignorable whitespace has been discarded, and is not present in the TF dataset.
Meaningful whitespace has been condensed to single spaces.

**Features**

feature | description
--- | ---
"""

class XML:
    def __init__( #initial features of the XML class;
                  #it can be changed with a script with the specific features of the data
        self,
        sourceVersion="0.1",
        testSet=set(),
        generic={},
        transform=None,
        tfVersion="0.1",
    ):
        (backend, org, repo, relative) = getLocation() #information of the file location
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

    @staticmethod #method that belongs to a class but does not depend on any instance or class variables
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
    def getParser(): #information for the XMLParser
        return etree.XMLParser(
            remove_blank_text=False,
            collect_ids=False,
            remove_comments=True,
            remove_pis=True,
            huge_tree=True,
        )

    def getXML(self): #obtain the xml file in a folder
        sourceDir = self.sourceDir
        testMode = self.testMode
        testSet = self.testSet

        xmlFilesRaw = []

        with scanDir(sourceDir) as fh:
            for file in fh:
                fileName = file.name
                if not (fileName.lower().endswith(".xml") and file.is_file()):
                    continue
                if testMode and fileName not in testSet:
                    continue
                xmlFilesRaw.append(fileName)

        return tuple(sorted(xmlFilesRaw))

    def checkTask(self): #check if the XML file and the converter is working properly
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
                tag = etree.QName(node.tag).localname #definition of the nodes
                atts = node.attrib #definition of the node attributes

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

        def writeReport(): #write the elements in each tag of all XML files analyzed
            reportFile = f"{reportDir}/elements.txt"
            with open(reportFile, "w", encoding="utf-8") as fh:
                fh.write(
                    "Inventory of tags and attributes in the source XML file(s).\n"
                )
                fh.write("\n\n")

                infoLines = 0

                def writeAttInfo(tag, att, attInfo): #writing attributes features in the elements.txt file
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

                def writeTagInfo(tag, tagInfo): #writing tag information in the elements.txt file
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
        for xmlFile in self.getXML():
            console(xmlFile)
            i += 1
            console(f"\r{i:>4} {xmlFile:<50}", newline=False)
            xmlPath = f"{sourceDir}/{xmlFile}"
            tree = etree.parse(xmlPath, parser)
            root = tree.getroot()
            analyse(root, analysis)
        
        console("")
        writeReport()

        return True

    # SET UP CONVERSION

    def getConverter(self): #for runing the conversion we need to call the conversion function (cv)
        tfPath = self.tfPath

        TF = Fabric(locations=tfPath)
        return CV(TF)

    def convertTask(self):
        slotType = "w" #variable that will run through all the slots (w means for word)
        otext = {
            "fmt:text-orig-full": "{text}{after}", #text format
            "sectionTypes": "book,chapter,verse", #type of section features
            "sectionFeatures": "book_short,chapter,verse" #section features as in the XML file
        }
        intFeatures = { #set of integer features
            "chapter",
            "verse",
            "book_num",
            "sentence_number",
            "nodeId",
            "strong",
            "word_in_verse"
        }
        featureMeta = dict( #metadata for the features with the description that will appear
                            #in the header of the tf files
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
            word_in_verse=dict(description="number of word")
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

        return cv.walk( #setting features for the convertion walker
            self.getDirector(),
            slotType,
            otext=otext,
            generic=generic,
            intFeatures=intFeatures,
            featureMeta=featureMeta,
            generateTf=True,
            warn=False #it indicates that even with a warning the code will continue running
        )

    # DIRECTOR

    def getDirector(self):
        """Factory for the director function.

        The `tf.convert.walker` relies on a corpus dependent `director` function
        that walks through the source data and spits out actions that
        produces the TF dataset.

        We collect all needed data, store it, and define a local director function
        that has access to this data.

        Returns
        -------
        function
            The local director function that has been constructed.
        """
        PASS_THROUGH = set( #set of elements which will be skipped by the walker
            """
            """.strip().split()
        )

        # CHECKING

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
            """Internal function to deal with a single element
            walking through the nodes.

            Will be called recursively.

            Parameters
            ----------
            cv: object
                The convertor object, needed to issue actions.
            cur: dict
                Various pieces of data collected during walking
                and relevant for some next steps in the walk.
            node: object
                An lxml element node.
            """
            tag = etree.QName(node.tag).localname #definition of the nodes
            cur["nest"].append(tag) #nest all tags in a dictionary

            beforeChildren(cv, cur, node, tag)

            for child in node.iterchildren(tag=etree.Element):
                walkNode(cv, cur, child) #it will walk recursevely to the children elements of the nodes

            afterChildren(cv, cur, node, tag)
            cur["nest"].pop()
            afterTag(cv, cur, node, tag)

        def addSlot(cv, cur, ch):
            """Add a slot.

            Whenever we encounter a character, we add it as a new slot.
            The slot (word) is saved and its feature is saved as a variable 'text'.

            Parameters
            ----------
            cv: object
                The convertor object, needed to issue actions.
            cur: dict
                Various pieces of data collected during walking
                and relevant for some next steps in the walk.
            ch: string
                A single character, the next slot in the result data.
            """

            s = cv.slot() #generating the slot
            cv.feature(s, text=ch) #it adds the feature 'text' to the slot

        def beforeChildren(cv, cur, node, tag):
            """Actions before dealing with the element's children.

            Parameters
            ----------
            cv: object
                The convertor object, needed to issue actions.
            cur: dict
                Various pieces of data collected during walking
                and relevant for some next steps in the walk.
            node: object
                An lxml element node.
            tag: string
                The tag of the lxml node.
            """
            if tag not in PASS_THROUGH:
                curNode = cv.node(tag) #obtaining information of the current node
                cur["elems"].append(curNode)
                atts = {etree.QName(k).localname: v for (k, v) in node.attrib.items()} #obtain the attributes of the current node
                if len(atts):
                    cv.feature(curNode, **atts)

            if tag == "word": #generating and saving the features 'chapter' and 'verse' as slots
                thisChapterNum = atts["chapter"]
                thisVerseNum = atts["verse"]
                if thisChapterNum != cv.get("chapter", cur["chapter"]):
                    if cur.get("verse", None) is not None:
                        cv.terminate(cur["verse"])
                    if cur.get("chapter", None) is not None:
                        cv.terminate(cur["chapter"])

                    curChapter = cv.node("chapter")
                    cur["chapter"] = curChapter
                    cv.feature(curChapter, chapter=thisChapterNum)

                    curVerse = cv.node("verse")
                    cur["verse"] = curVerse
                    cv.feature(curVerse, verse=thisVerseNum)

                elif thisVerseNum != cv.get("verse", cur["verse"]):
                    if cur.get("verse", None) is not None:
                        cv.terminate(cur["verse"])

                    curVerse = cv.node("verse")
                    cur["verse"] = curVerse
                    cv.feature(curVerse, verse=thisVerseNum)

                addSlot(cv, cur, atts["unicode"])   #generating the node of the word using the attribute 'unicode'

        def afterChildren(cv, cur, node, tag):
            """Node actions after dealing with the children, but before the end tag.

            Parameters
            ----------
            cv: object
                The convertor object, needed to issue actions.
            cur: dict
                Various pieces of data collected during walking
                and relevant for some next steps in the walk.
            node: object
                An lxml element node.
            tag: string
                The tag of the lxml node.
            """
            if tag not in PASS_THROUGH:
                curNode = cur["elems"].pop()
                atts = {etree.QName(k).localname: v for (k, v) in node.attrib.items()}

                if False and not cv.linked(curNode):
                    addSlot(cv, curNode, atts["after"])

                cv.terminate(curNode)

        def afterTag(cv, cur, node, tag):
            """Node actions after dealing with the children and after the end tag.

            This is the place where we proces the `tail` of an lxml node: the
            text material after the element and before the next open/close
            tag of any element.

            Parameters
            ----------
            cv: object
                The convertor object, needed to issue actions.
            cur: dict
                Various pieces of data collected during walking
                and relevant for some next steps in the walk.
            node: object
                An lxml element node.
            tag: string
                The tag of the lxml node.
            """

        def director(cv):
            """Director function.

            Here we program a walk through the XML sources.
            At every step of the walk we fire some actions that build TF nodes
            and assign features for them.

            Because everything is rather dynamic, we generate fairly standard
            metadata for the features.

            Parameters
            ----------
            cv: object
                The convertor object, needed to issue actions.
            """
            cur = {}

            i = 0
            for xmlFile in self.getXML():
                console(xmlFile)

                i += 1
                console(f"\r{i:>4} {xmlFile:<50}", newline=False)

                with open(f"{sourceDir}/{xmlFile}", encoding="utf-8") as fh:
                    #defining variables before reading the file
                    text = fh.read()
                    text = transformFunc(text)
                    tree = etree.parse(text, parser)
                    root = tree.getroot()
                    cur["nest"] = []
                    cur["elems"] = []
                    cur["chapter"] = None
                    cur["verse"] = None
                    walkNode(cv, cur, root) #walking though the nodes

                #addSlot(cv, cur, None) #remove 'None' that appears in the last line
                cv.terminate(cur["verse"]) #terminating the node 'verse'
                cv.terminate(cur["chapter"]) #terminanting the node 'chapter'

            console("")

            #dealing with feature metadata not indicated before
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
        """Implementation of the "load" task.

        It loads the tf data that resides in the directory where the "convert" task
        deliver its results.

        During loading there are additional checks. If they succeed, we have evidence
        that we have a valid TF dataset.

        Also, during the first load intensive precomputation of TF data takes place,
        the results of which will be cached in the invisible `.tf` directory there.

        That makes the TF data ready to be loaded fast, next time it is needed.

        Returns
        -------
        boolean
            Whether the loading was successful.
        """
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
        """Carry out any task, possibly modified by any flag.

        This is a higher level function that can execute a selection of tasks.

        The tasks will be executed in a fixed order: check, convert load.
        But you can select which one(s) must be executed.

        If multiple tasks must be executed and one fails, the subsequent tasks
        will not be executed.

        Parameters
        ----------
        check: boolean, optional False
            Whether to carry out the "check" task.
        convert: boolean, optional False
            Whether to carry out the "convert" task.
        load: boolean, optional False
            Whether to carry out the "load" task.
        test: boolean, optional None
            Whether to run in test mode.
            In test mode only the files in the test set are converted.

            If None, it will read its value from the attribute `testMode` of the
            `XML` object.

        Returns
        -------
        boolean
            Whether all tasks have executed successfully.
        """
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
        """Carry out tasks specified by arguments on the command line.

        The intended use of this module is that it is included by a conversion
        script.
        When that script is invoked, you can pass arguments to specify tasks
        and flags.

        This function inspects those arguments, and runs the specified tasks,
        with the specified flags enabled.

        Parameters
        ----------
        program: string
            The name of the program that you want to display
            in the help string, in case a help text must be displayed.

        Returns
        -------
        integer
            In fact, this function will terminate the conversion program
            an return a status code: 0 for succes, 1 for failure.
        """
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
