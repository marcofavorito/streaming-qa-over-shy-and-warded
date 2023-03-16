from pathlib import Path
from typing import Dict

from benchmark.datasets import DatasetID
from benchmark.datasets.classes.company_control import COMPANY_CONTROL_DATASET, COMPANY_CONTROL_PROGRAM
from benchmark.datasets.classes.dbpedia_psc import DBPEDIA_DATASET_DIR, DBPEDIA_PSC_PROGRAM
from benchmark.datasets.classes.dbpedia_stronglink import DBPEDIA_STRONGLINK_PROGRAM
from benchmark.datasets.classes.dbpedia_stronglink2 import DBPEDIA_STRONGLINK2_PROGRAM
from benchmark.datasets.classes.doctors import DOCTORS_DATASET_DIR, DOCTORS_FD_DATASET_DIR, DOCTORS_PROGRAM_DIR, \
    DOCTORS_FD_PROGRAM_DIR
from benchmark.datasets.classes.hasancestor import HAS_ANCESTOR_DATASET, HAS_ANCESTOR_PROGRAM
from benchmark.datasets.classes.lubm import LUBM_DATASET_DIR, LUBM_PROGRAM_DIR
from benchmark.datasets.classes.ontology_256 import ONTOLOGY_256_DATASET_DIR, ONTOLOGY_256_PROGRAM_DIR
from benchmark.datasets.classes.relationship import RELATIONSHIP_DATASET_PATH
from benchmark.datasets.classes.stb_128 import STB_128_DATASET_DIR, STB_128_PROGRAM_DIR
from benchmark.datasets.classes.synth import SYNTH_A_DATASET_DIR, SYNTH_B_DATASET_DIR, SYNTH_E_DATASET_DIR, \
    SYNTH_D_DATASET_DIR, SYNTH_C_DATASET_DIR, SYNTH_E_PROGRAM_DIR, SYNTH_D_PROGRAM_DIR, SYNTH_C_PROGRAM_DIR, \
    SYNTH_B_PROGRAM_DIR, SYNTH_A_PROGRAM_DIR


_dataset_path_by_dataset_id: Dict[DatasetID, Path] = {
    DatasetID.COMPANY_CONTROL: COMPANY_CONTROL_DATASET,
    DatasetID.DBPEDIA_PSC: DBPEDIA_DATASET_DIR,
    DatasetID.DBPEDIA_STRONGLINK: DBPEDIA_DATASET_DIR,
    DatasetID.DBPEDIA_STRONGLINK2: DBPEDIA_DATASET_DIR,
    DatasetID.DOCTORS: DOCTORS_DATASET_DIR,
    DatasetID.DOCTORS_FD: DOCTORS_FD_DATASET_DIR,
    DatasetID.LUBM: LUBM_DATASET_DIR,
    DatasetID.ONTOLOGY_256: ONTOLOGY_256_DATASET_DIR,
    DatasetID.RELATIONSHIP: RELATIONSHIP_DATASET_PATH,
    DatasetID.HAS_ANCESTOR: HAS_ANCESTOR_DATASET,
    DatasetID.STB_128: STB_128_DATASET_DIR,
    DatasetID.SYNTH_A: SYNTH_A_DATASET_DIR,
    DatasetID.SYNTH_B: SYNTH_B_DATASET_DIR,
    DatasetID.SYNTH_C: SYNTH_C_DATASET_DIR,
    DatasetID.SYNTH_D: SYNTH_D_DATASET_DIR,
    DatasetID.SYNTH_E: SYNTH_E_DATASET_DIR,
}


_program_path_by_dataset_id: Dict[DatasetID, Path] = {
    DatasetID.COMPANY_CONTROL: COMPANY_CONTROL_PROGRAM,
    DatasetID.DBPEDIA_PSC: DBPEDIA_PSC_PROGRAM,
    DatasetID.DBPEDIA_STRONGLINK: DBPEDIA_STRONGLINK_PROGRAM,
    DatasetID.DBPEDIA_STRONGLINK2: DBPEDIA_STRONGLINK2_PROGRAM,
    DatasetID.DOCTORS: DOCTORS_PROGRAM_DIR,
    DatasetID.DOCTORS_FD: DOCTORS_FD_PROGRAM_DIR,
    DatasetID.LUBM: LUBM_PROGRAM_DIR,
    DatasetID.ONTOLOGY_256: ONTOLOGY_256_PROGRAM_DIR,
    DatasetID.RELATIONSHIP: RELATIONSHIP_DATASET_PATH,
    DatasetID.HAS_ANCESTOR: HAS_ANCESTOR_PROGRAM,
    DatasetID.STB_128: STB_128_PROGRAM_DIR,
    DatasetID.SYNTH_A: SYNTH_A_PROGRAM_DIR,
    DatasetID.SYNTH_B: SYNTH_B_PROGRAM_DIR,
    DatasetID.SYNTH_C: SYNTH_C_PROGRAM_DIR,
    DatasetID.SYNTH_D: SYNTH_D_PROGRAM_DIR,
    DatasetID.SYNTH_E: SYNTH_E_PROGRAM_DIR,
}


def get_dataset_original_path(dataset_id: DatasetID):
    return _dataset_path_by_dataset_id[dataset_id]


def get_program_original_path(dataset_id: DatasetID):
    return _program_path_by_dataset_id[dataset_id]

