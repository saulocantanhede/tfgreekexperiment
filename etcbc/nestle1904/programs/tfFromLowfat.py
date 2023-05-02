import sys

from tf.convert.xml3 import XML
from tf.core.files import baseNm

if sys.stdout.encoding != 'utf-8':
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
if sys.stderr.encoding != 'utf-8':
    sys.stderr = open(sys.stderr.fileno(), mode='w', encoding='utf-8', buffering=1)


TEST_SET = set(
    """
    26-jude.xml
    """.strip().split()
)

AUTHOR = "Evangelists and apostles"
TITLE = "Greek New Testament"
INSTITUTE = "ETCBC (Eep Talstra Centre for Bible and Computer)"

GENERIC = dict(
    author=AUTHOR,
    title=TITLE,
    institute=INSTITUTE,
    language="nl",
    converters="Dirk Roorda et al. (Text-Fabric)",
    sourceFormat="XML (lowfat)",
    descriptionTf="Nestle 1904 edition",
)


def transform(text):
    return text


X = XML(
    sourceVersion="nestle1904",
    testSet=TEST_SET,
    generic=GENERIC,
    transform=transform,
    tfVersion="0.3",
)

X.run(baseNm(__file__))
