"""Build the Security Master from Security Candidates and raw OpenFIGI mappings."""
import argparse
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
import pandas as pd
from src.acquisition.acquisition_logger import log_preparation
from src.governance.source_registry import (
    ACTIVE_KEY, DATASET_KEY, PREPARATION_METHOD, PRODUCTION_METHOD_KEY,
    build_dataset_absolute_path, build_resolved_registry, get_dataset_by_id,
    get_input_dataset_configurations, get_parameters, get_preparation_enabled_datasets,
)
from src.preparation.exchange_mapping import build_yahoo_symbol, get_openfigi_exchange_code
from src.utils.logger import logger

SUCCESS_STATUS="Success"; FAILED_STATUS="Failed"
RESOLVED_STATUS="RESOLVED"; AMBIGUOUS_STATUS="AMBIGUOUS"; UNRESOLVED_STATUS="UNRESOLVED"
RULE_UNIQUE_FIGI="UNIQUE_FIGI"; RULE_EQUITY="MARKET_SECTOR_EQUITY"; RULE_EXCHANGE="EXCHANGE_MATCH"; RULE_TICKER="TICKER_MATCH"
RULE_EQUITY_EXCHANGE="MARKET_SECTOR_EQUITY_EXCHANGE_MATCH"; RULE_EQUITY_TICKER="MARKET_SECTOR_EQUITY_TICKER_MATCH"
RULE_EXCHANGE_TICKER="EXCHANGE_TICKER_MATCH"; RULE_EQUITY_EXCHANGE_TICKER="MARKET_SECTOR_EQUITY_EXCHANGE_TICKER_MATCH"
RULE_AMBIGUOUS="AMBIGUOUS_MULTIPLE_FIGIS"; RULE_UNRESOLVED_NO_MAPPING="UNRESOLVED_NO_MAPPING"
RULE_UNRESOLVED_PROVIDER_ERROR="UNRESOLVED_PROVIDER_ERROR"; RULE_UNRESOLVED_NO_FIGI="UNRESOLVED_NO_FIGI"

CANDIDATE_REQUIRED_COLUMNS=frozenset({"Ticker","Name","Location","Exchange","Currency","Asset Class"})
MAPPING_REQUIRED_COLUMNS=frozenset({"input_position","input_index","id_type","id_value","result_position","figi","ticker","name","exch_code","market_sector","security_type","security_type_2","security_description","composite_figi","share_class_figi","provider_warning","provider_error"})
OUTPUT_COLUMNS=("Ticker","Name","Location","Exchange","Currency","Asset Class","figi","composite_figi","share_class_figi","openfigi_ticker","openfigi_name","exch_code","market_sector","security_type","security_type_2","security_description","resolution_status","resolution_rule","mapping_candidates","expected_exch_code","yahoo_symbol")

def parse_arguments():
    p=argparse.ArgumentParser(description="Build a Security Master Dataset configured with Production Method=Preparation.")
    p.add_argument("--dataset-id",default=None)
    return p.parse_args()

def optional_text(value:Any)->str|None:
    if value is None:return None
    if not isinstance(value,(dict,list,tuple,set)):
        try:
            if pd.isna(value):return None
        except (TypeError,ValueError):pass
    x=str(value).strip(); return x or None

def required_text(value:Any,field_name:str)->str:
    x=optional_text(value)
    if x is None:raise ValueError(f"{field_name} is required.")
    return x

def normalize_position(value:Any,field_name:str)->int:
    if isinstance(value,bool):raise ValueError(f"{field_name} must be a non-negative integer.")
    try:n=float(value); i=int(n)
    except (TypeError,ValueError) as e:raise ValueError(f"{field_name} must be a non-negative integer.") from e
    if n!=i or i<0:raise ValueError(f"{field_name} must be a non-negative integer.")
    return i

def validate_preparation_configuration(c:dict[str,Any])->None:
    if not isinstance(c,dict):raise TypeError("configuration must be a dictionary.")
    did=required_text(c.get(DATASET_KEY),DATASET_KEY); method=required_text(c.get(PRODUCTION_METHOD_KEY),PRODUCTION_METHOD_KEY)
    if method.casefold()!=PREPARATION_METHOD.casefold():raise ValueError(f"{did} is not configured for Preparation.")
    if not isinstance(c.get(ACTIVE_KEY),bool):raise TypeError(f"{did}: Active must be boolean.")
    if not c[ACTIVE_KEY]:raise ValueError(f"{did} is inactive.")
    pars=get_parameters(c)
    if not isinstance(pars,dict):raise TypeError("Security Master Parameters must be a dictionary.")
    if pars:raise ValueError("Security Master currently expects empty Parameters.")

def select_preparation_configuration(dataset_id=None,*,registry=None)->dict[str,Any]:
    r=build_resolved_registry() if registry is None else registry.copy(deep=True)
    requested=optional_text(dataset_id)
    if requested is not None:
        c=get_dataset_by_id(requested,registry=r).to_dict(); validate_preparation_configuration(c); return c
    candidates=get_preparation_enabled_datasets(registry=r)
    if len(candidates)!=1:raise ValueError("Provide --dataset-id when active Preparation Dataset count is not exactly one.")
    c=candidates.iloc[0].to_dict(); validate_preparation_configuration(c); return c

def read_tabular_dataset(path:Path)->pd.DataFrame:
    if not path.exists():raise FileNotFoundError(path)
    ext=path.suffix.casefold()
    if ext==".csv":return pd.read_csv(path,dtype=str,keep_default_na=False,encoding="utf-8")
    if ext==".json":return pd.read_json(path)
    if ext==".parquet":return pd.read_parquet(path)
    raise ValueError(f"Unsupported Security Master input extension: {ext}")

def dataframe_supports_columns(df,required):return required.issubset(set(df.columns))

def resolve_input_datasets(c,*,registry):
    did=required_text(c.get(DATASET_KEY),DATASET_KEY); configs=get_input_dataset_configurations(did,registry=registry)
    if len(configs)!=2:raise ValueError(f"{did}: Security Master requires exactly two upstream Datasets. Resolved inputs: {len(configs)}")
    loaded=[]
    for conf in configs:
        path=build_dataset_absolute_path(conf); loaded.append((path,read_tabular_dataset(path)))
    cm=[x for x in loaded if dataframe_supports_columns(x[1],CANDIDATE_REQUIRED_COLUMNS) and not dataframe_supports_columns(x[1],MAPPING_REQUIRED_COLUMNS)]
    mm=[x for x in loaded if dataframe_supports_columns(x[1],MAPPING_REQUIRED_COLUMNS)]
    if len(cm)!=1 or len(mm)!=1:raise ValueError("Unable to identify Security Candidates and OpenFIGI Mapping inputs uniquely by schema.")
    out=build_dataset_absolute_path(c); paths=[x[0] for x in loaded]
    if out in paths:raise ValueError("Security Master output path cannot equal an input path.")
    return cm[0][1].copy(),mm[0][1].copy(),paths,out

def normalize_candidate_frame(df):
    missing=sorted(CANDIDATE_REQUIRED_COLUMNS-set(df.columns))
    if missing:raise ValueError(f"Security Candidates is missing columns: {missing}")
    f=df.copy(deep=True)
    for col in CANDIDATE_REQUIRED_COLUMNS:f[col]=f[col].fillna("").astype(str).str.strip()
    if f.empty:raise ValueError("Security Candidates Dataset is empty.")
    f=f.reset_index(drop=True); f.insert(0,"_candidate_position",range(len(f))); return f

def normalize_mapping_frame(df):
    missing=sorted(MAPPING_REQUIRED_COLUMNS-set(df.columns))
    if missing:raise ValueError(f"OpenFIGI Mapping is missing columns: {missing}")
    f=df.copy(deep=True)
    for col in ("id_type","id_value","figi","ticker","name","exch_code","market_sector","security_type","security_type_2","security_description","composite_figi","share_class_figi","provider_warning","provider_error"):
        f[col]=f[col].fillna("").astype(str).str.strip()
    f["input_position"]=f["input_position"].apply(lambda v:normalize_position(v,"input_position")); return f

def validate_mapping_lineage(candidates,mappings):
    if not mappings["input_position"].between(0,len(candidates)-1).all():raise ValueError("OpenFIGI Mapping input_position is outside candidate range.")
    bad=[]
    for _,m in mappings.iterrows():
        p=int(m["input_position"]); iv=optional_text(m["id_value"])
        if iv and candidates.iloc[p]["Ticker"].casefold()!=iv.casefold():bad.append((p,candidates.iloc[p]["Ticker"],iv))
    if bad:raise ValueError(f"OpenFIGI Mapping lineage mismatch: {bad[:20]}")

def distinct_figi_candidates(m):
    x=m[m["figi"].ne("")].copy()
    if x.empty:return x
    return x.sort_values("result_position",kind="stable").drop_duplicates("figi",keep="first").reset_index(drop=True)

def _prefer(m,mask):
    x=m[mask]
    if x.empty or len(x)==len(m):return m.copy(),False
    return x.copy(),True

def prefer_equity(m):return _prefer(m,m["market_sector"].str.casefold().eq("equity"))
def prefer_exchange(m,code):
    if code is None:return m.copy(),False
    return _prefer(m,m["exch_code"].str.casefold().eq(code.casefold()))
def prefer_ticker(m,ticker):return _prefer(m,m["ticker"].str.casefold().eq(required_text(ticker,"Ticker").casefold()))

def determine_resolution_rule(*,equity_applied,exchange_applied,ticker_applied):
    key=(equity_applied,exchange_applied,ticker_applied)
    return {(1,1,1):RULE_EQUITY_EXCHANGE_TICKER,(1,1,0):RULE_EQUITY_EXCHANGE,(1,0,1):RULE_EQUITY_TICKER,(0,1,1):RULE_EXCHANGE_TICKER,(1,0,0):RULE_EQUITY,(0,1,0):RULE_EXCHANGE,(0,0,1):RULE_TICKER}.get(key,RULE_UNIQUE_FIGI)

def empty_openfigi_fields():
    return {"figi":"","composite_figi":"","share_class_figi":"","openfigi_ticker":"","openfigi_name":"","exch_code":"","market_sector":"","security_type":"","security_type_2":"","security_description":""}

def mapping_output_fields(m):
    return {"figi":m["figi"],"composite_figi":m["composite_figi"],"share_class_figi":m["share_class_figi"],"openfigi_ticker":m["ticker"],"openfigi_name":m["name"],"exch_code":m["exch_code"],"market_sector":m["market_sector"],"security_type":m["security_type"],"security_type_2":m["security_type_2"],"security_description":m["security_description"]}

def resolve_candidate(candidate,mappings):
    ticker=required_text(candidate["Ticker"],"Ticker"); exchange=required_text(candidate["Exchange"],"Exchange")
    expected=get_openfigi_exchange_code(exchange); yahoo=build_yahoo_symbol(ticker,exchange)
    out={k:candidate[k] for k in ("Ticker","Name","Location","Exchange","Currency","Asset Class")}; out.update(empty_openfigi_fields()); out["expected_exch_code"]=expected or ""; out["yahoo_symbol"]=yahoo or ""
    if mappings.empty:out.update(resolution_status=UNRESOLVED_STATUS,resolution_rule=RULE_UNRESOLVED_NO_MAPPING,mapping_candidates=0);return out
    usable=mappings[mappings["provider_error"].eq("")].copy()
    if usable.empty:out.update(resolution_status=UNRESOLVED_STATUS,resolution_rule=RULE_UNRESOLVED_PROVIDER_ERROR,mapping_candidates=0);return out
    usable=distinct_figi_candidates(usable)
    if usable.empty:out.update(resolution_status=UNRESOLVED_STATUS,resolution_rule=RULE_UNRESOLVED_NO_FIGI,mapping_candidates=0);return out
    if len(usable)==1:
        out.update(mapping_output_fields(usable.iloc[0]));out.update(resolution_status=RESOLVED_STATUS,resolution_rule=RULE_UNIQUE_FIGI,mapping_candidates=1);return out
    usable,e=prefer_equity(usable); usable=distinct_figi_candidates(usable)
    if len(usable)==1:
        out.update(mapping_output_fields(usable.iloc[0]));out.update(resolution_status=RESOLVED_STATUS,resolution_rule=determine_resolution_rule(equity_applied=e,exchange_applied=False,ticker_applied=False),mapping_candidates=1);return out
    usable,x=prefer_exchange(usable,expected); usable=distinct_figi_candidates(usable)
    if len(usable)==1:
        out.update(mapping_output_fields(usable.iloc[0]));out.update(resolution_status=RESOLVED_STATUS,resolution_rule=determine_resolution_rule(equity_applied=e,exchange_applied=x,ticker_applied=False),mapping_candidates=1);return out
    usable,t=prefer_ticker(usable,ticker); usable=distinct_figi_candidates(usable)
    if len(usable)==1:
        out.update(mapping_output_fields(usable.iloc[0]));out.update(resolution_status=RESOLVED_STATUS,resolution_rule=determine_resolution_rule(equity_applied=e,exchange_applied=x,ticker_applied=t),mapping_candidates=1);return out
    out.update(resolution_status=AMBIGUOUS_STATUS,resolution_rule=RULE_AMBIGUOUS,mapping_candidates=len(usable));return out

def build_security_master_dataframe(candidates,mappings):
    candidates=normalize_candidate_frame(candidates); mappings=normalize_mapping_frame(mappings); validate_mapping_lineage(candidates,mappings)
    groups={int(p):g.copy(deep=True) for p,g in mappings.groupby("input_position",sort=False)}; empty=mappings.iloc[0:0].copy(); records=[]
    for _,c in candidates.iterrows():records.append(resolve_candidate(c,groups.get(int(c["_candidate_position"]),empty)))
    master=pd.DataFrame(records,columns=OUTPUT_COLUMNS)
    if len(master)!=len(candidates):raise RuntimeError("Security Master row count does not match Security Candidates.")
    missing=master[master["yahoo_symbol"].eq("")][["Ticker","Exchange"]]
    if not missing.empty:raise RuntimeError(f"Security Master contains unresolved Yahoo symbols: {missing.to_dict(orient='records')}")
    dup=master["yahoo_symbol"].duplicated(keep=False)
    if dup.any():raise RuntimeError(f"Security Master contains duplicate Yahoo symbols: {master.loc[dup,['Ticker','Exchange','yahoo_symbol']].to_dict(orient='records')}")
    counts=master["resolution_status"].value_counts().to_dict()
    meta={"candidate_records":len(candidates),"mapping_records":len(mappings),"output_records":len(master),"resolved_records":int(counts.get(RESOLVED_STATUS,0)),"ambiguous_records":int(counts.get(AMBIGUOUS_STATUS,0)),"unresolved_records":int(counts.get(UNRESOLVED_STATUS,0)),"yahoo_symbol_records":int(master["yahoo_symbol"].ne("").sum())}
    return master,meta

def write_csv_atomically(df,output_path):
    if output_path.suffix.casefold()!=".csv":raise ValueError("Security Master output must be CSV.")
    output_path.parent.mkdir(parents=True,exist_ok=True); tmp=None
    try:
        with NamedTemporaryFile(mode="wb",prefix=f".{output_path.stem}_",suffix=output_path.suffix,dir=output_path.parent,delete=False) as f:tmp=Path(f.name)
        df.to_csv(tmp,index=False,encoding="utf-8"); v=pd.read_csv(tmp,dtype=str,keep_default_na=False,encoding="utf-8")
        if list(v.columns)!=list(OUTPUT_COLUMNS) or len(v)!=len(df):raise RuntimeError("Generated Security Master failed staging verification.")
        os.replace(tmp,output_path); tmp=None
    finally:
        if tmp is not None and tmp.exists():tmp.unlink(missing_ok=True)

def build_security_master(dataset_id=None)->Path:
    registry=build_resolved_registry(); c=select_preparation_configuration(dataset_id,registry=registry); did=required_text(c.get(DATASET_KEY),DATASET_KEY)
    candidates,mappings,input_paths,output_path=resolve_input_datasets(c,registry=registry)
    logger.info("Security Master preparation started: Dataset=%s",did)
    try:
        master,meta=build_security_master_dataframe(candidates,mappings); write_csv_atomically(master,output_path)
        notes=json.dumps({"inputs":[p.name for p in input_paths],**meta},ensure_ascii=False,sort_keys=True)
        run_id=log_preparation(output_file=output_path,storage_location=output_path.parent,status=SUCCESS_STATUS,records_produced=len(master),notes=notes)
        logger.info("Security Master completed: Records=%s; Resolved=%s; Ambiguous=%s; Unresolved=%s; YahooSymbols=%s",meta["output_records"],meta["resolved_records"],meta["ambiguous_records"],meta["unresolved_records"],meta["yahoo_symbol_records"])
        logger.info("Preparation Run ID: %s",run_id); return output_path
    except Exception as error:
        logger.exception("Security Master preparation failed: Dataset=%s; Error=%s",did,error)
        try:log_preparation(output_file=output_path,storage_location=output_path.parent,status=FAILED_STATUS,records_produced=None,notes=json.dumps({"dataset_id":did,"error_type":type(error).__name__,"error":str(error)},ensure_ascii=False,sort_keys=True))
        except Exception as logging_error:logger.exception("Unable to persist failed Security Master preparation: %s",logging_error)
        raise

def main():
    args=parse_arguments(); output=build_security_master(dataset_id=args.dataset_id); logger.info("Security Master Dataset published: %s",output)

if __name__=="__main__":
    main()
