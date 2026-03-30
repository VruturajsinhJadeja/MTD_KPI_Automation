import json
import requests
import urllib.parse
import pytz
import sys
import os
from datetime import datetime
from collections import defaultdict

# IMPORT THE AUTH UTIL
from auth_util import AuthUtil

# ==========================================
# FORMATTING & COLORS
# ==========================================
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    DEBUG = '\033[90m'

def print_header(msg):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*60}\n{msg}\n{'='*60}{Colors.ENDC}")

def print_pass(msg):
    print(f"{Colors.OKGREEN}[PASS] {msg}{Colors.ENDC}")

def print_fail(msg):
    print(f"{Colors.FAIL}[FAIL] {msg}{Colors.ENDC}")

def print_info(msg):
    print(f"{Colors.OKCYAN}[INFO] {msg}{Colors.ENDC}")

def print_debug(msg):
    print(f"{Colors.DEBUG}{msg}{Colors.ENDC}")

# ==========================================
# DATE HELPER
# ==========================================
def get_month_range_utc(timezone_str):
    try:
        tz = pytz.timezone(timezone_str)
    except pytz.UnknownTimeZoneError:
        print(f"{Colors.WARNING}Timezone '{timezone_str}' invalid, defaulting to UTC{Colors.ENDC}")
        tz = pytz.utc

    now = datetime.now(tz)
    first_day = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    if first_day.month == 12:
        next_month = first_day.replace(year=first_day.year + 1, month=1)
    else:
        next_month = first_day.replace(month=first_day.month + 1)

    utc_fmt = "%Y-%m-%d %H:%M:%S"
    start_utc = first_day.astimezone(pytz.utc).strftime(utc_fmt)
    end_utc = next_month.astimezone(pytz.utc).strftime(utc_fmt)
    
    return start_utc, end_utc

# ==========================================
# ORDER AGGREGATOR (PHASE 1)
# ==========================================
class GlobalOrderFetcher:
    VALID_STATUSES = {"ENTREGADO", "RENDIDO"}

    def __init__(self, env_config, global_registry):
        self.base_url = env_config['base_url']
        self.lob = env_config['lob']
        self.timezone_str = env_config.get('timezone', 'UTC')
        self.global_registry = global_registry # Shared Dictionary
        self.month_start_utc, _ = get_month_range_utc(self.timezone_str)

    def fetch_orders(self, login_id, token):
        headers = {
            "Authorization": token,
            "Content-Type": "application/json",
            "Lob": self.lob
        }
        
        page = 0
        has_more = True
        page_size = 1000
        count = 0

        print_info(f"    Fetching orders for user: {login_id}")

        while has_more:
            filter_str = f"creationTime[gte]:{self.month_start_utc}"
            sort_param = urllib.parse.quote("creationTime:desc")
            filter_param = urllib.parse.quote(filter_str)

            endpoint = f"/v2/orders?size={page_size}&page={page}&sort={sort_param}&filter={filter_param}"
            full_url = f"{self.base_url}{endpoint}"

            # Optional: Uncomment to debug specific user calls
            # self._print_curl("GET", full_url, headers)

            resp = requests.get(full_url, headers=headers)
            if resp.status_code != 200:
                print_fail(f"    Error fetching orders for {login_id}: {resp.status_code}")
                break

            features = resp.json().get("features", [])
            if not features:
                break

            self._process_orders(features)
            count += len(features)
            has_more = len(features) == page_size
            page += 1
        
        print(f"    -> Parsed {count} orders.")

    def _process_orders(self, features):
        for order in features:
            status = order.get("status", "").strip().upper()
            if status not in self.VALID_STATUSES:
                continue

            # NOTE: Removed loginId filter. 
            # We aggregate ALL valid orders visible to this user.

            outlet = self._extract_outlet(order)
            if not outlet: continue

            details = order.get("orderDetails", [])
            for d in details:
                sku = self._extract_sku(d)
                if sku:
                    # Add to GLOBAL registry
                    self.global_registry[outlet].add(sku)

    def _extract_outlet(self, obj):
        if val := obj.get("outletCode", "").strip(): return val
        try:
            return obj["extendedAttributes"]["shipmentDetails"]["outletCode"].strip()
        except (KeyError, TypeError):
            return ""

    def _extract_sku(self, obj):
        if val := obj.get("skuCode", "").strip(): return val
        if val := obj.get("sku", "").strip(): return val
        try:
            return obj["extendedAttributes"]["skuCode"].strip()
        except (KeyError, TypeError):
            return ""

    def _print_curl(self, method, url, headers, body=None):
        curl_cmd = f"curl --location --request {method} '{url}'"
        for k, v in headers.items():
            safe_v = v.replace("'", "'\\''")
            curl_cmd += f" \\\n--header '{k}: {safe_v}'"
        print_debug(curl_cmd)

# ==========================================
# KPI VALIDATOR (PHASE 2)
# ==========================================
class KpiValidator:
    KPI_NAME = "kpi_review_sales_value_outlet_list_raw_data"

    def __init__(self, env_config, global_registry):
        self.base_url = env_config['base_url']
        self.lob = env_config['lob']
        self.global_ordered_skus = global_registry # The Master List
        self.kpi_skus_by_outlet = defaultdict(set)
        self.errors = []

    def validate_user(self, login_id, token):
        self.kpi_skus_by_outlet.clear()
        self.errors = []
        
        print_info(f"    Fetching KPI for user: {login_id}")
        
        # 1. Fetch KPI
        self._fetch_kpi(token)

        # 2. Compare against GLOBAL orders
        return self._validate_consistency(login_id)

    def _fetch_kpi(self, token):
        headers = {
            "Authorization": token,
            "Content-Type": "application/json",
            "Lob": self.lob
        }
        body = json.dumps({"kpis": [{"kpiName": self.KPI_NAME}]})
        full_url = f"{self.base_url}/v1/kpidata/data"

        # Optional: Debug KPI curl
        # self._print_curl("POST", full_url, headers, body)

        resp = requests.post(full_url, headers=headers, data=body)
        if resp.status_code != 200:
            raise Exception(f"KPI API Error: {resp.status_code}")

        kpis = resp.json().get("kpis", [])
        target_kpi = next((k for k in kpis if k.get("kpiName") == self.KPI_NAME), None)

        if not target_kpi:
            raise Exception(f"KPI '{self.KPI_NAME}' not found.")

        rows = target_kpi.get("response", [])
        for row in rows:
            outlet = row.get("outletCode", "").strip()
            if not outlet: continue
            for sku in row.get("skus_billed", []):
                s = str(sku).strip()
                if s: self.kpi_skus_by_outlet[outlet].add(s)

    def _validate_consistency(self, login_id):
        # We compare the User's KPI view against the GLOBAL Reality
        
        # Identify relevant outlets: All outlets that have orders globally OR appear in this user's KPI
        # Note: If an outlet has Global Orders, but doesn't appear in User A's KPI, 
        # that implies User A cannot see that outlet (or the KPI is broken).
        
        relevant_outlets = set(self.global_ordered_skus.keys()) | set(self.kpi_skus_by_outlet.keys())

        for outlet in relevant_outlets:
            ordered_set = self.global_ordered_skus.get(outlet, set()) # Global Truth
            kpi_set = self.kpi_skus_by_outlet.get(outlet, set())      # User View

            # CASE 1: Outlet has Orders Globally, but missing from User's KPI
            if outlet in self.global_ordered_skus and outlet not in self.kpi_skus_by_outlet:
                # Warning: This might just mean the User isn't assigned to this outlet.
                # However, based on requirements, we treat this as a consistency check.
                self.errors.append({
                    "type": "MISSING_OUTLET",
                    "outlet": outlet,
                    "ordered": ordered_set,
                    "kpi": set()
                })
                continue

            # CASE 2: Ghost Outlet (In KPI, but NO orders globally)
            if outlet in self.kpi_skus_by_outlet and outlet not in self.global_ordered_skus:
                 self.errors.append({
                    "type": "GHOST_OUTLET",
                    "outlet": outlet,
                    "ordered": set(),
                    "kpi": kpi_set
                })
                 continue

            # CASE 3: SKU Mismatch
            missing_in_kpi = ordered_set - kpi_set
            extra_in_kpi = kpi_set - ordered_set

            if missing_in_kpi or extra_in_kpi:
                self.errors.append({
                    "type": "SKU_MISMATCH",
                    "outlet": outlet,
                    "ordered": ordered_set,
                    "kpi": kpi_set,
                    "missing": missing_in_kpi,
                    "extra": extra_in_kpi
                })

        if self.errors:
            self._print_error_report(login_id)
            return False
        return True

    def _print_error_report(self, login_id):
        print(f"\n{Colors.FAIL}Errors found for User {login_id}:{Colors.ENDC}")
        for i, err in enumerate(self.errors, 1):
            outlet = err['outlet']
            # Sort for clean display
            ordered_list = sorted(list(err['ordered']))
            kpi_list = sorted(list(err['kpi']))
            
            print(f"  {i}. Outlet: {Colors.BOLD}{outlet}{Colors.ENDC}")
            
            if err['type'] == "MISSING_OUTLET":
                print(f"     Expected (Global Orders): {ordered_list}")
                print(f"     Actual   (User KPI)     : []")
                print(f"     {Colors.FAIL}[CRITICAL] Outlet missing from KPI.{Colors.ENDC}")
            
            elif err['type'] == "GHOST_OUTLET":
                print(f"     Expected (Global Orders): []")
                print(f"     Actual   (User KPI)     : {kpi_list}")
                print(f"     {Colors.FAIL}[CRITICAL] KPI shows data, but no Global Orders found.{Colors.ENDC}")
            
            elif err['type'] == "SKU_MISMATCH":
                missing = err.get('missing')
                extra = err.get('extra')
                
                print(f"     Expected (Global Orders): {ordered_list}")
                print(f"     Actual   (User KPI)     : {kpi_list}")
                
                if missing:
                    print(f"     {Colors.FAIL}[Data Loss] Missing in KPI: {sorted(list(missing))}{Colors.ENDC}")
                if extra:
                    print(f"     {Colors.WARNING}[Ghost Data] Extra in KPI: {sorted(list(extra))}{Colors.ENDC}")

    def _print_curl(self, method, url, headers, body=None):
        curl_cmd = f"curl --location --request {method} '{url}'"
        for k, v in headers.items():
            safe_v = v.replace("'", "'\\''")
            curl_cmd += f" \\\n--header '{k}: {safe_v}'"
        if body:
            safe_body = body.replace("'", "'\\''")
            curl_cmd += f" \\\n--data-raw '{safe_body}'"
        print_debug(f"\n[DEBUG CURL]:\n{curl_cmd}\n")

# ==========================================
# MAIN EXECUTION
# ==========================================
def load_config():
    config_path = "config.json"
    if not os.path.exists(config_path):
        print_fail(f"Configuration file '{config_path}' not found.")
        sys.exit(1)
    with open(config_path, 'r') as f:
        return json.load(f)

if __name__ == "__main__":
    config = load_config()
    env_config = config['environment']
    users = config['users']

    auth_util = AuthUtil(
        base_url=env_config['base_url'],
        lob=env_config['lob'],
        public_key_str=env_config['public_key']
    )

    # SHARED MEMORY FOR ALL USERS
    # Format: global_orders[outlet_code] = {sku1, sku2, ...}
    global_orders = defaultdict(set)

    print_header(f"PHASE 1: AGGREGATING GLOBAL ORDERS\nDate Range: {get_month_range_utc(env_config.get('timezone', 'UTC'))}")
    
    # PHASE 1: COLLECT ORDERS
    fetcher = GlobalOrderFetcher(env_config, global_orders)
    
    # We need a cache of tokens to avoid re-login in Phase 2
    user_tokens = {}

    for user in users:
        login_id = user['login_id']
        password = user['password']
        
        try:
            token = auth_util.generate_token(login_id, password)
            user_tokens[login_id] = token
            fetcher.fetch_orders(login_id, token)
        except Exception as e:
            print_fail(f"    Failed to fetch orders for {login_id}: {str(e)}")

    total_outlets = len(global_orders)
    print_info(f"Global Aggregation Complete. Found valid orders for {total_outlets} outlets across all users.")

    if total_outlets == 0:
        print(f"{Colors.WARNING}No orders found globally. Expecting empty KPIs.{Colors.ENDC}")

    # PHASE 2: VALIDATE KPIS
    print_header("PHASE 2: VALIDATING USER KPIS AGAINST GLOBAL TRUTH")
    
    validator = KpiValidator(env_config, global_orders)
    results = {"passed": 0, "failed": 0, "skipped": 0}

    for user in users:
        login_id = user['login_id']
        token = user_tokens.get(login_id)
        
        if not token:
            results["skipped"] += 1
            continue

        try:
            success = validator.validate_user(login_id, token)
            if success:
                print_pass(f"User {login_id} KPI matches Global Orders.")
                results["passed"] += 1
            else:
                print_fail(f"User {login_id} KPI failed consistency check.")
                results["failed"] += 1
        except Exception as e:
            print_fail(f"Error validating User {login_id}: {str(e)}")
            results["failed"] += 1

    # SUMMARY
    print_header("BATCH EXECUTION SUMMARY")
    print(f"{Colors.OKGREEN}Passed:      {results['passed']}{Colors.ENDC}")
    print(f"{Colors.FAIL}Failed:      {results['failed']}{Colors.ENDC}")
    print(f"{Colors.WARNING}Skipped:     {results['skipped']}{Colors.ENDC}")

    sys.exit(1 if results["failed"] > 0 else 0)