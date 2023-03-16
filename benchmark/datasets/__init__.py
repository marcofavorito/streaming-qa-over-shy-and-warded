from benchmark.datasets.classes.company_control import CompanyControlDataset
from benchmark.datasets.classes.dbpedia_psc import DBPediaPscDataset
from benchmark.datasets.classes.dbpedia_stronglink import DBPediaStronglinkDataset
from benchmark.datasets.classes.dbpedia_stronglink2 import DBPediaStronglink2Dataset
from benchmark.datasets.classes.doctors import DoctorsDataset
from benchmark.datasets.classes.hasancestor import HasAncestorDataset
from benchmark.datasets.classes.lubm import LUBMDataset
from benchmark.datasets.classes.ontology_256 import Ontology256Dataset
from benchmark.datasets.classes.relationship import RelationshipDataset
from benchmark.datasets.classes.stb_128 import STB128Dataset
from benchmark.datasets.classes.synth import SynthDataset, SynthADataset, SynthBDataset, SynthCDataset, SynthDDataset, \
    SynthEDataset
from benchmark.datasets.core import DatasetRegistry, DatasetID

dataset_registry = DatasetRegistry()


dataset_registry.register(
    DatasetID.COMPANY_CONTROL,
    item_cls=CompanyControlDataset,
)
dataset_registry.register(
    DatasetID.DBPEDIA_PSC,
    item_cls=DBPediaPscDataset,
)
dataset_registry.register(
    DatasetID.DBPEDIA_STRONGLINK,
    item_cls=DBPediaStronglinkDataset,
)
dataset_registry.register(
    DatasetID.DBPEDIA_STRONGLINK2,
    item_cls=DBPediaStronglink2Dataset,
)
dataset_registry.register(
    DatasetID.DOCTORS,
    item_cls=DoctorsDataset,
)
dataset_registry.register(
    DatasetID.DOCTORS_FD,
    item_cls=DoctorsDataset,
)
dataset_registry.register(
    DatasetID.LUBM,
    item_cls=LUBMDataset,
)
dataset_registry.register(
    DatasetID.ONTOLOGY_256,
    item_cls=Ontology256Dataset,
)
dataset_registry.register(
    DatasetID.RELATIONSHIP,
    item_cls=RelationshipDataset,
)
dataset_registry.register(
    DatasetID.HAS_ANCESTOR,
    item_cls=HasAncestorDataset,
)
dataset_registry.register(
    DatasetID.STB_128,
    item_cls=STB128Dataset,
)
dataset_registry.register(
    DatasetID.SYNTH_A,
    item_cls=SynthADataset,
)
dataset_registry.register(
    DatasetID.SYNTH_B,
    item_cls=SynthBDataset,
)
dataset_registry.register(
    DatasetID.SYNTH_C,
    item_cls=SynthCDataset,
)
dataset_registry.register(
    DatasetID.SYNTH_D,
    item_cls=SynthDDataset,
)
dataset_registry.register(
    DatasetID.SYNTH_E,
    item_cls=SynthEDataset,
)
