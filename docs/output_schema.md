# Output schema

Run `tdcpass pipeline run` to produce the quarterly panel, estimates, diagnostics, manifests and local site data. Paths below are relative to the run root. The configuration specifies minimum CSV columns and top-level JSON keys. [Machine-readable output contract](../config/output_contract.yml) · [Quarterly pipeline](../src/tdcpass/pipeline/quarterly.py)

Treasury Contribution to Deposits (TDC) and total deposit change share a unit, billions of US dollars. Subtracting TDC from total deposit change gives the non-TDC component. An accounting reconstruction tests whether components add up. A direct-source comparison measures how much of that residual independently observed sources cover. A response to unexpected TDC describes the fitted relationship. Causal and mechanism interpretations depend on the identification and coverage diagnostics.

Jump to the [shared CSV columns](#shared-csv-columns), [JSON fields](#json-fields) or [local site copies](#local-site-copies).

## Required files

### `data/derived/quarterly_panel.csv`

One row per quarter. The panel joins quarterly Treasury and bank series with macro controls. It computes `other_component_qoq` as `total_deposits_bank_qoq` minus `tdc_bank_only_qoq`. The headline sample requires complete baseline series and lags. Shorter proxy histories retain missing observations.

Each lagged column contains the preceding quarter’s value in the same units. `bill_share` divides bill offerings by all Treasury auction offerings grouped by issue quarter. `reserve_drain_pressure` equals the negative of `lag_reserves_qoq`. `quarter_index` counts quarters from zero. The aliases `tdc_qoq` and `total_deposits_qoq` refer to the bank-only TDC and total-deposit series. MMF denotes a money market fund, RRP a reverse repurchase agreement, DI a depository institution and CU a credit union. The shock fit exports the standardized residual `tdc_residual_z` in the shock table. [Series registry](../config/series_registry.yml) · [Panel construction](../src/tdcpass/pipeline/build_panel.py)

| Column | One-quarter lag | Meaning | Unit |
| --- | --- | --- | --- |
| `quarter` | — | Calendar quarter | YYYYQn |
| `tdc_bank_only_qoq` | `lag_tdc_bank_only_qoq` | TDC, bank-only, quarterly change | USD billions |
| `tdc_tier2_bank_only_qoq` | — | TDC, legacy Tier 2 H15/WAMEST interest-corrected bank-only, quarterly change | USD billions |
| `tdc_tier2_di_np_cu_qoq` | — | TDC, legacy Tier 2 depository-institution NP-CU candidate, quarterly change | USD billions |
| `tdc_tier2_modern_canonical_di_mmf_rrp_prop_qoq` | — | TDC, short modern canonical Tier 2 DI/MMF-RRP, quarterly change | USD billions |
| `tdc_tier2_regression_mmf_rrp_prop_bank_only_qoq` | — | TDC, long-history Tier 2 regression/MMF-RRP bank-only, quarterly change | USD billions |
| `tdc_tier2_regression_mmf_rrp_lb_bank_only_qoq` | — | Tier 2 regression MMF/RRP lower bound, bank-only | USD billions |
| `tdc_tier2_regression_mmf_rrp_ub_bank_only_qoq` | — | Tier 2 regression MMF/RRP upper bound, bank-only | USD billions |
| `tdc_tier2_regression_mmf_rrp_prop_di_np_cu_qoq` | — | TDC, long-history Tier 2 regression/MMF-RRP DI NP-CU, quarterly change | USD billions |
| `tdc_tier2_regression_mmf_rrp_lb_di_np_cu_qoq` | — | Tier 2 regression MMF/RRP lower bound, depository institutions, NP-CU | USD billions |
| `tdc_tier2_regression_mmf_rrp_ub_di_np_cu_qoq` | — | Tier 2 regression MMF/RRP upper bound, depository institutions, NP-CU | USD billions |
| `tdc_tier2_regression_bank_row_method_tier` | — | Method tier for the regression-based bank and ROW terms | Label |
| `tdc_tier2_regression_di_method_tier` | — | Method tier for the regression-based DI estimate | Label |
| `tdc_tier2_mmf_rrp_prop_bank_only_qoq` | — | TDC, Tier 2 plus MMF/RRP proportional adjustment, bank-only, quarterly change | USD billions |
| `tdc_tier2_mmf_rrp_lb_bank_only_qoq` | — | TDC, Tier 2 plus MMF/RRP lower bound, bank-only, quarterly change | USD billions |
| `tdc_tier2_mmf_rrp_ub_bank_only_qoq` | — | TDC, Tier 2 plus MMF/RRP upper bound, bank-only, quarterly change | USD billions |
| `tdc_tier2_mmf_rrp_prop_di_np_cu_qoq` | — | TDC, Tier 2 plus MMF/RRP proportional adjustment, DI NP-CU, quarterly change | USD billions |
| `tdc_tier2_treasury_interest_robust_bank_only_qoq` | — | TDC, Tier 2 bill-discount robustness, bank-only, quarterly change | USD billions |
| `tdc_tier2_treasury_interest_robust_mmf_rrp_prop_bank_only_qoq` | — | TDC, Tier 2 plus MMF/RRP bill-discount robustness, bank-only, quarterly change | USD billions |
| `tdc_tier2_treasury_interest_robust_di_np_cu_qoq` | — | TDC, Tier 2 bill-discount robustness, DI NP-CU, quarterly change | USD billions |
| `tdc_tier2_treasury_interest_robust_mmf_rrp_prop_di_np_cu_qoq` | — | TDC, Tier 2 plus MMF/RRP bill-discount robustness, DI NP-CU, quarterly change | USD billions |
| `tdc_tier3_bank_only_qoq` | — | TDC, Tier 3 fiscal-corrected bank-only, quarterly change | USD billions |
| `tdc_tier3_broad_depository_qoq` | — | TDC, Tier 3 fiscal-corrected broad depository, quarterly change | USD billions |
| `tdc_bank_receipt_historical_overlay_qoq` | — | Historical bank-receipt overlay | USD billions |
| `tdc_row_mrv_nondefault_pilot_qoq` | — | Alternative ROW pilot estimate | USD billions |
| `total_deposits_bank_qoq` | `lag_total_deposits_bank_qoq` | Total bank deposits, matched quarterly change | USD billions |
| `deposits_only_bank_qoq` | `lag_deposits_only_bank_qoq` | Deposits only, excluding interbank liabilities, quarterly change | USD billions |
| `broad_bank_deposits_qoq` | `lag_broad_bank_deposits_qoq` | Broad-bank deposits across matched bank sectors, quarterly change | USD billions |
| `checkable_deposits_bank_qoq` | `lag_checkable_deposits_bank_qoq` | Bank checkable deposits, quarterly change | USD billions |
| `checkable_deposits_foreign_offices_qoq` | `lag_checkable_deposits_foreign_offices_qoq` | Foreign banking offices checkable deposits, quarterly change | USD billions |
| `checkable_deposits_affiliated_areas_qoq` | `lag_checkable_deposits_affiliated_areas_qoq` | Banks in U.S.-affiliated areas checkable deposits, quarterly change | USD billions |
| `interbank_transactions_bank_qoq` | `lag_interbank_transactions_bank_qoq` | Bank interbank transactions liabilities, quarterly change | USD billions |
| `time_savings_deposits_bank_qoq` | `lag_time_savings_deposits_bank_qoq` | Bank time and savings deposits, quarterly change | USD billions |
| `time_savings_deposits_foreign_offices_qoq` | `lag_time_savings_deposits_foreign_offices_qoq` | Foreign banking offices time and savings deposits, quarterly change | USD billions |
| `time_savings_deposits_affiliated_areas_qoq` | `lag_time_savings_deposits_affiliated_areas_qoq` | Banks in U.S.-affiliated areas time and savings deposits, quarterly change | USD billions |
| `checkable_federal_govt_bank_qoq` | `lag_checkable_federal_govt_bank_qoq` | Bank checkable federal government deposits, quarterly change | USD billions |
| `checkable_state_local_bank_qoq` | `lag_checkable_state_local_bank_qoq` | Bank checkable state and local deposits, quarterly change | USD billions |
| `checkable_rest_of_world_bank_qoq` | `lag_checkable_rest_of_world_bank_qoq` | Bank checkable rest-of-world deposits, quarterly change | USD billions |
| `checkable_private_domestic_bank_qoq` | `lag_checkable_private_domestic_bank_qoq` | Bank checkable private domestic deposits, quarterly change | USD billions |
| `z1_households_nonprofits_nonfinancial_business_currency_deposit_assets_qoq` | — | Household/nonprofit and nonfinancial-business currency-and-deposit assets, quarterly change | USD billions |
| `interbank_transactions_foreign_banks_liability_qoq` | `lag_interbank_transactions_foreign_banks_liability_qoq` | Bank interbank liabilities to foreign banks, quarterly change | USD billions |
| `interbank_transactions_foreign_banks_asset_qoq` | `lag_interbank_transactions_foreign_banks_asset_qoq` | Bank interbank assets on foreign banks, quarterly change | USD billions |
| `deposits_at_foreign_banks_asset_qoq` | `lag_deposits_at_foreign_banks_asset_qoq` | Bank deposits at foreign banks, quarterly change | USD billions |
| `other_component_qoq` | — | Non-TDC deposit component, quarterly change | USD billions |
| `accounting_deposit_substitution_qoq` | — | Accounting deposit-substitution component, quarterly change | USD billions |
| `accounting_bank_balance_sheet_qoq` | — | Accounting bank balance-sheet component, quarterly change | USD billions |
| `accounting_public_liquidity_qoq` | — | Accounting public-liquidity component, quarterly change | USD billions |
| `accounting_external_flow_qoq` | — | Accounting external-flow component, quarterly change | USD billions |
| `accounting_identity_total_qoq` | — | Accounting identity reconstructed total, quarterly change | USD billions |
| `accounting_identity_gap_qoq` | — | Accounting identity closure gap, quarterly change | USD billions |
| `strict_loan_source_qoq` | `lag_strict_loan_source_qoq` | Strict identifiable loan-source lane, quarterly flow | USD billions |
| `strict_loan_mortgages_qoq` | `lag_strict_loan_mortgages_qoq` | Strict loan-source mortgages component, quarterly flow | USD billions |
| `strict_loan_consumer_credit_qoq` | `lag_strict_loan_consumer_credit_qoq` | Strict loan-source consumer-credit component, quarterly flow | USD billions |
| `strict_loan_di_loans_nec_qoq` | `lag_strict_loan_di_loans_nec_qoq` | Strict loan-source DI loans n.e.c. component, quarterly flow | USD billions |
| `strict_di_loans_nec_households_nonprofits_qoq` | `lag_strict_di_loans_nec_households_nonprofits_qoq` | Strict DI loans n.e.c. households and nonprofits borrower counterpart, quarterly flow | USD billions |
| `strict_di_loans_nec_nonfinancial_corporate_qoq` | `lag_strict_di_loans_nec_nonfinancial_corporate_qoq` | Strict DI loans n.e.c. nonfinancial corporate borrower counterpart, quarterly flow | USD billions |
| `strict_di_loans_nec_nonfinancial_noncorporate_qoq` | `lag_strict_di_loans_nec_nonfinancial_noncorporate_qoq` | Strict DI loans n.e.c. nonfinancial noncorporate borrower counterpart, quarterly flow | USD billions |
| `strict_di_loans_nec_state_local_qoq` | `lag_strict_di_loans_nec_state_local_qoq` | Strict DI loans n.e.c. state and local borrower counterpart, quarterly flow | USD billions |
| `strict_di_loans_nec_domestic_financial_qoq` | `lag_strict_di_loans_nec_domestic_financial_qoq` | Strict DI loans n.e.c. domestic financial borrower counterpart, quarterly flow | USD billions |
| `strict_di_loans_nec_rest_of_world_qoq` | `lag_strict_di_loans_nec_rest_of_world_qoq` | Strict DI loans n.e.c. rest of world borrower counterpart, quarterly flow | USD billions |
| `strict_di_loans_nec_systemwide_liability_total_qoq` | `lag_strict_di_loans_nec_systemwide_liability_total_qoq` | Strict DI loans n.e.c. systemwide liability total, quarterly flow | USD billions |
| `strict_di_loans_nec_systemwide_borrower_total_qoq` | `lag_strict_di_loans_nec_systemwide_borrower_total_qoq` | Strict DI loans n.e.c. systemwide borrower diagnostic total, quarterly flow | USD billions |
| `strict_di_loans_nec_systemwide_borrower_gap_qoq` | `lag_strict_di_loans_nec_systemwide_borrower_gap_qoq` | Strict DI loans n.e.c. systemwide borrower diagnostic gap, quarterly flow | USD billions |
| `strict_di_loans_nec_private_domestic_borrower_qoq` | `lag_strict_di_loans_nec_private_domestic_borrower_qoq` | Strict DI loans n.e.c. private domestic borrower counterpart subtotal, quarterly flow | USD billions |
| `strict_di_loans_nec_noncore_system_borrower_qoq` | `lag_strict_di_loans_nec_noncore_system_borrower_qoq` | Strict DI loans n.e.c. noncore system borrower counterpart subtotal, quarterly flow | USD billions |
| `strict_loan_other_advances_qoq` | `lag_strict_loan_other_advances_qoq` | Strict loan-source other loans and advances component, quarterly flow | USD billions |
| `strict_loan_core_min_qoq` | `lag_strict_loan_core_min_qoq` | Strict redesigned loan core, direct minimum, quarterly flow | USD billions |
| `strict_loan_core_plus_private_borrower_qoq` | `lag_strict_loan_core_plus_private_borrower_qoq` | Strict redesigned loan core with private-borrower add-on, quarterly flow | USD billions |
| `strict_loan_core_plus_nonfinancial_corporate_qoq` | `lag_strict_loan_core_plus_nonfinancial_corporate_qoq` | Strict redesigned loan core with nonfinancial-corporate add-on, quarterly flow | USD billions |
| `strict_loan_noncore_system_qoq` | `lag_strict_loan_noncore_system_qoq` | Strict noncore/system loan diagnostic subtotal, quarterly flow | USD billions |
| `strict_non_treasury_agency_gse_qoq` | `lag_strict_non_treasury_agency_gse_qoq` | Strict non-Treasury agency and GSE securities component, quarterly flow | USD billions |
| `strict_non_treasury_municipal_qoq` | `lag_strict_non_treasury_municipal_qoq` | Strict non-Treasury municipal securities component, quarterly flow | USD billions |
| `strict_non_treasury_corporate_foreign_bonds_qoq` | `lag_strict_non_treasury_corporate_foreign_bonds_qoq` | Strict non-Treasury corporate and foreign bonds component, quarterly flow | USD billions |
| `strict_non_treasury_securities_qoq` | `lag_strict_non_treasury_securities_qoq` | Strict direct non-Treasury securities lane, quarterly flow | USD billions |
| `strict_identifiable_total_qoq` | `lag_strict_identifiable_total_qoq` | Strict identifiable direct total, quarterly flow | USD billions |
| `strict_identifiable_gap_qoq` | `lag_strict_identifiable_gap_qoq` | Strict identifiable gap, quarterly flow | USD billions |
| `strict_funding_fedfunds_repo_qoq` | `lag_strict_funding_fedfunds_repo_qoq` | Strict funding fed-funds and repo liabilities, quarterly flow | USD billions |
| `strict_funding_debt_securities_qoq` | `lag_strict_funding_debt_securities_qoq` | Strict funding debt-securities liabilities, quarterly flow | USD billions |
| `strict_funding_fhlb_advances_qoq` | `lag_strict_funding_fhlb_advances_qoq` | Strict funding FHLB advances and Sallie Mae loans, quarterly flow | USD billions |
| `strict_funding_offset_total_qoq` | `lag_strict_funding_offset_total_qoq` | Strict funding-offset total, quarterly flow | USD billions |
| `strict_identifiable_net_after_funding_qoq` | `lag_strict_identifiable_net_after_funding_qoq` | Strict identifiable total net of funding offsets, quarterly flow | USD billions |
| `strict_gap_after_funding_qoq` | `lag_strict_gap_after_funding_qoq` | Strict gap after funding offsets, quarterly flow | USD billions |
| `bank_credit_private_qoq` | `lag_bank_credit_private_qoq` | Bank private-credit / non-Treasury proxy, quarterly change | USD billions |
| `commercial_industrial_loans_qoq` | `lag_commercial_industrial_loans_qoq` | Commercial and industrial loans, quarterly change | USD billions |
| `construction_land_development_loans_qoq` | `lag_construction_land_development_loans_qoq` | Construction and land development loans, quarterly change | USD billions |
| `cre_multifamily_loans_qoq` | `lag_cre_multifamily_loans_qoq` | Multifamily CRE loans, quarterly change | USD billions |
| `cre_nonfarm_nonresidential_loans_qoq` | `lag_cre_nonfarm_nonresidential_loans_qoq` | Nonfarm nonresidential CRE loans, quarterly change | USD billions |
| `consumer_loans_qoq` | `lag_consumer_loans_qoq` | Consumer loans, quarterly change | USD billions |
| `credit_card_revolving_loans_qoq` | `lag_credit_card_revolving_loans_qoq` | Credit-card and revolving consumer loans, quarterly change | USD billions |
| `auto_loans_qoq` | `lag_auto_loans_qoq` | Automobile loans, quarterly change | USD billions |
| `other_consumer_loans_qoq` | `lag_other_consumer_loans_qoq` | Other consumer loans, quarterly change | USD billions |
| `heloc_loans_qoq` | `lag_heloc_loans_qoq` | Revolving home equity loans, quarterly change | USD billions |
| `closed_end_residential_loans_qoq` | `lag_closed_end_residential_loans_qoq` | Closed-end residential loans, quarterly change | USD billions |
| `loans_to_commercial_banks_qoq` | `lag_loans_to_commercial_banks_qoq` | Loans to commercial banks, quarterly change | USD billions |
| `loans_to_nondepository_financial_institutions_qoq` | `lag_loans_to_nondepository_financial_institutions_qoq` | Loans to nondepository financial institutions, quarterly change | USD billions |
| `loans_for_purchasing_or_carrying_securities_qoq` | `lag_loans_for_purchasing_or_carrying_securities_qoq` | Loans for purchasing or carrying securities, quarterly change | USD billions |
| `treasury_securities_bank_qoq` | `lag_treasury_securities_bank_qoq` | Bank Treasury securities, quarterly change | USD billions |
| `agency_gse_backed_securities_bank_qoq` | `lag_agency_gse_backed_securities_bank_qoq` | Bank agency- and GSE-backed securities, quarterly change | USD billions |
| `municipal_securities_bank_qoq` | `lag_municipal_securities_bank_qoq` | Bank municipal securities, quarterly change | USD billions |
| `corporate_foreign_bonds_bank_qoq` | `lag_corporate_foreign_bonds_bank_qoq` | Bank corporate and foreign bonds, quarterly change | USD billions |
| `fedfunds_repo_liabilities_bank_qoq` | `lag_fedfunds_repo_liabilities_bank_qoq` | Bank fed funds and repo liabilities, quarterly change | USD billions |
| `commercial_bank_borrowings_qoq` | `lag_commercial_bank_borrowings_qoq` | Commercial-bank borrowings, quarterly change | USD billions |
| `fed_borrowings_depository_institutions_qoq` | `lag_fed_borrowings_depository_institutions_qoq` | Depository-institution Fed borrowings, quarterly change | USD billions |
| `debt_securities_bank_liability_qoq` | `lag_debt_securities_bank_liability_qoq` | Bank debt securities liabilities, quarterly change | USD billions |
| `fhlb_advances_sallie_mae_loans_bank_qoq` | `lag_fhlb_advances_sallie_mae_loans_bank_qoq` | Bank FHLB advances and Sallie Mae loans, quarterly change | USD billions |
| `holding_company_parent_funding_bank_qoq` | `lag_holding_company_parent_funding_bank_qoq` | Bank holding-company parent funding, quarterly change | USD billions |
| `commercial_industrial_loans_ex_chargeoffs_qoq` | `lag_commercial_industrial_loans_ex_chargeoffs_qoq` | Commercial and industrial loans, quarterly change plus charge-off add-back | USD billions |
| `consumer_loans_ex_chargeoffs_qoq` | `lag_consumer_loans_ex_chargeoffs_qoq` | Consumer loans, quarterly change plus charge-off add-back | USD billions |
| `credit_card_revolving_loans_ex_chargeoffs_qoq` | `lag_credit_card_revolving_loans_ex_chargeoffs_qoq` | Credit-card and revolving consumer loans, quarterly change plus charge-off add-back | USD billions |
| `other_consumer_loans_ex_chargeoffs_qoq` | `lag_other_consumer_loans_ex_chargeoffs_qoq` | Other consumer loans, quarterly change plus charge-off add-back | USD billions |
| `closed_end_residential_loans_ex_chargeoffs_qoq` | `lag_closed_end_residential_loans_ex_chargeoffs_qoq` | Closed-end residential loans, quarterly change plus charge-off add-back | USD billions |
| `cb_nonts_qoq` | — | Central-bank non-Treasury channel proxy, quarterly change | USD billions |
| `foreign_nonts_qoq` | — | Foreign non-Treasury channel proxy, quarterly change | USD billions |
| `domestic_nonfinancial_mmf_reallocation_qoq` | — | Domestic nonfinancial MMF reallocation proxy, quarterly change | USD billions |
| `domestic_nonfinancial_repo_reallocation_qoq` | — | Domestic nonfinancial repo reallocation proxy, quarterly change | USD billions |
| `on_rrp_reallocation_qoq` | `lag_on_rrp_reallocation_qoq` | ON RRP reallocation support proxy, quarterly change | USD billions |
| `household_treasury_securities_reallocation_qoq` | `lag_household_treasury_securities_reallocation_qoq` | Household Treasury-holdings reallocation support proxy, quarterly change | USD billions |
| `mmf_treasury_bills_reallocation_qoq` | `lag_mmf_treasury_bills_reallocation_qoq` | MMF Treasury-bill reallocation support proxy, quarterly change | USD billions |
| `currency_reallocation_qoq` | `lag_currency_reallocation_qoq` | Public currency reallocation support proxy, quarterly change | USD billions |
| `other_component_no_foreign_bank_sectors_qoq` | — | Non-TDC component under no-foreign-bank-sectors TDC sensitivity, quarterly change | USD billions |
| `other_component_no_toc_bank_only_qoq` | — | Non-TDC component under no-TOC TDC sensitivity, quarterly change | USD billions |
| `other_component_no_toc_no_row_bank_only_qoq` | — | Non-TDC component under no-TOC/no-ROW TDC sensitivity, quarterly change | USD billions |
| `other_component_core_deposit_proximate_bank_only_qoq` | — | Non-TDC component under deposit-proximate core TDC, quarterly change | USD billions |
| `broad_bank_other_component_qoq` | — | Broad-bank non-TDC component under headline TDC, quarterly change | USD billions |
| `broad_strict_loan_foreign_offices_qoq` | `lag_broad_strict_loan_foreign_offices_qoq` | Broad strict foreign-banking-offices loan source, quarterly flow | USD billions |
| `broad_strict_loan_affiliated_areas_qoq` | `lag_broad_strict_loan_affiliated_areas_qoq` | Broad strict affiliated-areas bank loan source, quarterly flow | USD billions |
| `broad_strict_loan_source_qoq` | `lag_broad_strict_loan_source_qoq` | Broad matched-scope strict loan source, quarterly flow | USD billions |
| `broad_strict_gap_qoq` | `lag_broad_strict_gap_qoq` | Broad matched-scope strict gap, quarterly flow | USD billions |
| `bill_share` | `lag_bill_share` | Bill-heavy issuance share | Fraction |
| `reserve_drain_pressure` | — | Reserve-drain pressure | USD billions |
| `quarter_index` | — | Quarter index | Index |
| `tga_qoq` | `lag_tga_qoq` | Treasury General Account quarterly change | USD billions |
| `reserves_qoq` | `lag_reserves_qoq` | Reserve balances quarterly change | USD billions |
| `tdc_domestic_bank_only_qoq` | — | TDC, bank-only excluding rest of world, quarterly change | USD billions |
| `tdc_us_chartered_bank_only_qoq` | `lag_tdc_us_chartered_bank_only_qoq` | TDC, U.S.-chartered bank-leg-matched, quarterly change | USD billions |
| `tdc_no_foreign_bank_sectors_qoq` | `lag_tdc_no_foreign_bank_sectors_qoq` | TDC, excluding foreign bank sectors but keeping ROW, quarterly change | USD billions |
| `tdc_no_toc_bank_only_qoq` | `lag_tdc_no_toc_bank_only_qoq` | TDC, bank-only excluding Treasury operating cash, quarterly change | USD billions |
| `tdc_no_toc_no_row_bank_only_qoq` | `lag_tdc_no_toc_no_row_bank_only_qoq` | TDC, bank-only excluding Treasury operating cash and ROW, quarterly change | USD billions |
| `tdc_core_deposit_proximate_bank_only_qoq` | `lag_tdc_core_deposit_proximate_bank_only_qoq` | TDC core, deposit-proximate bank-only quarterly change | USD billions |
| `tdc_toc_row_support_bundle_qoq` | `lag_tdc_toc_row_support_bundle_qoq` | TDC TOC/ROW support bundle, quarterly change | USD billions |
| `tdc_no_remit_bank_only_qoq` | — | Bank-only TDC excluding remittances | USD billions |
| `tdc_credit_union_sensitive_qoq` | — | Credit-union aggregate TDC sensitivity | USD billions |
| `fedfunds` | `lag_fedfunds` | Federal Funds Rate | Percent |
| `unemployment` | `lag_unemployment` | Unemployment rate | Percent |
| `inflation` | `lag_inflation` | Inflation / CPI growth | Percent change |

### `data/derived/call_report_deposit_components.csv`

One row per quarter, account type, depositor class and covered universe. The importer normalizes Call Report deposit balances to billions of US dollars and aggregates them. With no source file, the export contains only its header.

Columns — `quarter`, `account_type`, `depositor_class`, `amount_bil_usd`, `institution_count`, `source_quarter`, `source_kind`, `universe_basis`.

### `output/accounting/accounting_summary.csv`

Descriptive statistics over quarters with all three accounting series present. The three means are in billions of US dollars per quarter. The negative-residual share is a fraction of quarters, and the two correlations are dimensionless.

Columns — `metric`, `value`, `notes`.

### `output/accounting/quarters_tdc_exceeds_total.csv`

Quarterly panel rows where the Treasury contribution exceeds the total change in deposits and the arithmetic residual is negative. All three amounts are in billions of US dollars.

Columns — `quarter`, `tdc_bank_only_qoq`, `total_deposits_bank_qoq`, `other_component_qoq`.

### `output/shocks/unexpected_tdc.csv`

One row per quarter. The baseline ridge regression predicts bank-only TDC from lagged TDC, the federal funds rate, unemployment and inflation. Each fit uses 24 to 40 complete earlier quarters. Predictions and residuals are missing before enough observations accumulate.

Columns — `quarter`, `tdc_bank_only_qoq`, `tdc_fitted`, `tdc_residual`, `tdc_residual_z`, `model_name`, `train_start_obs`, `train_condition_number`, `train_target_sd`, `train_resid_sd`, `fitted_to_target_scale_ratio`, `fitted_to_train_target_sd_ratio`, `shock_flag`.

### `output/models/lp_irf.csv`

Local projections by outcome, specification and horizon. The baseline can include a separate lag of each dependent variable. The resulting equations can have different regressors and samples.

Columns — `outcome`, `horizon`, `beta`, `se`, `lower95`, `upper95`, `n`, `spec_name`, `shock_column`, `shock_scale`, `response_type`.

### `output/models/lp_irf_identity_baseline.csv`

Fits TDC and total deposits using the same observations, horizon transform and regressors. Subtracting the two fitted responses gives the other-component response. The shared bootstrap supplies uncertainty for this difference.

Columns — `outcome`, `horizon`, `beta`, `se`, `lower95`, `upper95`, `n`, `spec_name`, `decomposition_mode`, `outcome_construction`, `inference_method`.

### `output/models/lp_irf_accounting_identity.csv`

Selects baseline local projections for the non-TDC residual, its accounting components, their reconstructed total and the closure gap. Closure measures arithmetic consistency.

Columns — `outcome`, `horizon`, `beta`, `se`, `lower95`, `upper95`, `n`, `spec_name`.

### `output/models/lp_irf_strict_identifiable.csv`

Selects baseline local projections for the non-TDC residual, directly measured loan flows, non-Treasury securities, their gross total and the uncovered gap.

Columns — `outcome`, `horizon`, `beta`, `se`, `lower95`, `upper95`, `n`, `spec_name`.

### `output/models/accounting_identity_alignment.csv`

Joins the residual, reconstructed total and accounting-gap responses at each headline horizon. Compares residual minus reconstructed total with the directly estimated gap.

Columns — `horizon`, `residual_beta`, `accounting_total_beta`, `identity_gap_beta`, `arithmetic_residual_minus_total_beta`, `identity_gap_share_of_residual`, `residual_n`, `accounting_total_n`, `identity_gap_n`, `interpretation`.

### `output/models/strict_identifiable_alignment.csv`

Joins the loan, securities and residual responses by horizon. Reports the gross amount accounted for by the measured sources and the remaining gap as a share of the residual.

Columns — `horizon`, `residual_beta`, `strict_loan_source_beta`, `strict_non_treasury_securities_beta`, `strict_identifiable_total_beta`, `strict_identifiable_gap_beta`, `arithmetic_residual_minus_total_beta`, `strict_gap_share_of_residual`, `residual_n`, `strict_total_n`, `strict_gap_n`, `interpretation`.

### `output/models/strict_funding_offset_alignment.csv`

Compares gross source responses with the funding offset, the net amount after funding and the remaining gap at each horizon.

Columns — `horizon`, `strict_identifiable_total_beta`, `strict_funding_offset_total_beta`, `strict_funding_offset_share_of_identifiable_total_beta`, `strict_identifiable_net_after_funding_beta`, `strict_gap_after_funding_beta`, `identifiable_total_n`, `funding_total_n`, `net_after_funding_n`, `gap_after_funding_n`, `interpretation`.

### `output/models/accounting_identity_summary.json`

Summarizes the horizon-by-horizon accounting reconstruction, identifies available component outcomes and records the reconstruction source.

Fields — `status`, `source_kind`, `headline_question`, `estimation_path`, `component_outcomes_present`, `horizons`, `takeaways`.

### `output/models/strict_identifiable_summary.json`

Summarizes the directly measured loan and securities responses at each horizon and the portion of the non-TDC residual they cover.

Fields — `status`, `source_kind`, `headline_question`, `estimation_path`, `component_outcomes_present`, `horizons`, `takeaways`.

### `output/models/strict_identifiable_followup_summary.json`

Compares strict estimates across TDC definitions, decomposes loans and funding offsets, and compares the US-chartered lender asset with systemwide borrower liabilities. When available, `scope_check_gap_assessment` holds the strict total fixed and changes the residual perimeter. `recommended_measurement_comparison` labels the matched-bank and no-ROW comparisons.

Fields — `status`, `strict_source_kind`, `headline_question`, `estimation_path`, `measurement_variant_comparison`, `strict_component_diagnostics`, `di_loans_nec_borrower_diagnostics`, `funding_offset_sensitivity`, `takeaways`.

### `output/models/scope_alignment_summary.json`

Compares bank-only TDC, the no-ROW variant (`domestic_bank_only`) and the matched bank-leg variant (`us_chartered_bank_only`) against deposits with and without interbank transaction liabilities. `recommended_policy`, when present, identifies the headline deposit and comparison perimeters.

Fields — `status`, `headline_question`, `variant_definitions`, `deposit_concepts`, `takeaways`.

### `output/models/broad_scope_system_summary.json`

Pairs broad TDC with deposits from US-chartered banks, foreign banking offices and banks in US-affiliated areas. Compares the resulting residual with a loans-only core and separates foreign bank-sector transactions from the rest-of-world leg.

Fields — `status`, `headline_question`, `estimation_path`, `usc_matched_context`, `broad_matched_system`, `tdc_component_audit`, `takeaways`.

### `output/models/treasury_operating_cash_audit_summary.json`

Compares Treasury operating cash (TOC) with the Treasury General Account (TGA), reserves and deposit outcomes using same-quarter movements and local projections. The `treasury_cash_release_pattern` label summarizes the observed sign pattern for `tdc_treasury_operating_cash_qoq`.

Fields — `status`, `headline_question`, `estimation_path`, `quarterly_alignment`, `key_horizons`, `takeaways`.

### `output/models/rest_of_world_treasury_audit_summary.json`

Compares rest-of-world (ROW) Treasury transactions with foreign-sector deposits and other external asset movements, both quarter by quarter and in shock responses. The audit labels a broader external asset pattern for `tdc_row_treasury_transactions_qoq` as `external_asset_support_pattern`.

Fields — `status`, `headline_question`, `estimation_path`, `quarterly_alignment`, `key_horizons`, `takeaways`.

### `output/models/toc_row_bundle_audit_summary.json`

Subtracts TOC from ROW and measures how the signed bundle aligns with in-scope deposits, liquidity and external series in the panel and across response horizons. The `broad_support_bundle_pattern` label identifies a broader support response.

Fields — `status`, `headline_question`, `estimation_path`, `quarterly_alignment`, `key_horizons`, `takeaways`.

### `output/models/toc_row_path_split_summary.json`

Compares the combined TOC and ROW legs with direct deposit and broader support paths. The path labels are `direct_deposit_path_dominant`, `broad_support_path_dominant` and `mixed_path_signal`.

Fields — `status`, `headline_question`, `estimation_path`, `path_definitions`, `quarterly_split`, `key_horizons`, `takeaways`.

### `output/models/toc_row_excluded_interpretation_summary.json`

Re-estimates responses with TOC and ROW removed from TDC and compares the resulting deposit residual and direct-count gap with the baseline.

Fields — `status`, `headline_question`, `estimation_path`, `comparison_definition`, `key_horizons`, `takeaways`.

### `output/models/strict_missing_channel_summary.json`

Compares the residual after excluding TOC and ROW with the loan core, securities and funding offsets. The remaining gap measures the part outside those observed channels.

Fields — `status`, `headline_question`, `estimation_path`, `comparison_definition`, `key_horizons`, `takeaways`.

### `output/models/strict_sign_mismatch_audit_summary.json`

Aligns the baseline and TOC/ROW-excluded shocks, locates quarters contributing to their difference, and compares the signs of strict component responses under both shocks.

Fields — `status`, `headline_question`, `estimation_path`, `shock_alignment`, `quarter_concentration`, `gap_driver_alignment`, `component_alignment`, `interpretation`, `takeaways`.

### `output/models/strict_shock_composition_summary.json`

Groups the largest differences between baseline and excluded shocks by quarter and period. Repeats the comparison after trimming high-gap observations or a period bucket.

Fields — `status`, `headline_question`, `estimation_path`, `top_gap_quarters`, `period_bucket_profiles`, `trim_diagnostics`, `interpretation`, `takeaways`.

### `output/models/strict_top_gap_quarter_audit_summary.json`

Decomposes high-gap quarters into TOC and ROW contributions, labels the magnitude split as `toc_dominant`, `row_dominant` or `mixed`, and classifies the signed contributions as `reinforcing`, `offsetting` or `single_leg`.

Fields — `status`, `headline_question`, `estimation_path`, `top_gap_quarters`, `dominant_leg_summary`, `contribution_pattern_summary`, `interpretation`, `takeaways`.

### `output/models/strict_top_gap_quarter_direction_summary.json`

Compares the sign of each high-gap quarter’s shock difference with the signs of TOC, ROW and their sum. Direction labels include `aligned`, `opposed` and `neutral`. The driver distinguishes `both_legs_align_gap`, `both_legs_oppose_gap`, `toc_driven_gap_direction` and `row_driven_gap_direction`.

Fields — `status`, `headline_question`, `estimation_path`, `top_gap_quarters`, `gap_bundle_alignment_summary`, `directional_driver_summary`, `interpretation`, `takeaways`.

### `output/models/strict_top_gap_inversion_summary.json`

Groups high-gap quarters by the direction of the TOC and ROW contributions, then summarizes sign combinations such as `negative_residual_positive_strict` and `positive_residual_positive_strict`.

Fields — `status`, `headline_question`, `estimation_path`, `top_gap_quarters`, `directional_driver_context_summary`, `residual_strict_pattern_summary`, `interpretation`, `takeaways`.

### `output/models/strict_top_gap_anomaly_summary.json`

Compares the selected anomalous quarter with peers in its direction bucket. Reports weighted peer means, signed deviations and the largest component differences.

Fields — `status`, `headline_question`, `estimation_path`, `anomaly_quarter`, `peer_quarters`, `peer_pattern_summary`, `weighted_peer_means`, `anomaly_vs_peer_deltas`, `ranked_anomaly_component_deltas`, `interpretation`, `takeaways`.

### `output/models/strict_top_gap_anomaly_component_split_summary.json`

Breaks the anomaly-minus-peer deviation into loan, securities, funding, liquidity and external components. Ranks component differences by magnitude.

Fields — `status`, `headline_question`, `estimation_path`, `anomaly_quarter`, `peer_quarters`, `peer_bucket_weight`, `loan_subcomponent_deltas`, `securities_subcomponent_deltas`, `funding_subcomponent_deltas`, `liquidity_external_deltas`, `ranked_component_deltas`, `interpretation`, `takeaways`.

### `output/models/strict_top_gap_anomaly_di_loans_split_summary.json`

Splits the difference in depository-institution (DI) loans not elsewhere classified by borrower sector. Compares the largest sector contribution and the borrower reconciliation gap with peers.

Fields — `status`, `headline_question`, `estimation_path`, `anomaly_quarter`, `peer_quarters`, `peer_bucket_weight`, `di_loans_nec_component_deltas`, `dominant_borrower_component`, `borrower_gap_row`, `interpretation`, `takeaways`.

### `output/models/strict_top_gap_anomaly_backdrop_summary.json`

Compares corporate credit in the anomalous quarter with peer levels of loans, reserves, foreign-sector balances, TGA and the residual. The absolute-magnitude ratio scales liquidity and external differences to the corporate difference.

Fields — `status`, `headline_question`, `estimation_path`, `anomaly_quarter`, `peer_quarters`, `peer_bucket_weight`, `backdrop_rows`, `corporate_credit_row`, `loan_source_row`, `reserves_row`, `foreign_nonts_row`, `tga_row`, `residual_row`, `liquidity_external_abs_to_corporate_abs_ratio`, `interpretation`, `takeaways`.

### `output/models/big_picture_synthesis_summary.json`

Combines impact-horizon estimates with the high-gap decomposition and anomaly comparison. Classifies the contributions of perimeter differences and TOC/ROW to the measured residual.

Fields — `status`, `headline_question`, `estimation_path`, `h0_snapshot`, `quarter_composition`, `supporting_case`, `classification`, `interpretation`, `takeaways`.

### `output/models/treatment_object_comparison_summary.json`

Compares the broad bank-only TDC series with alternative treatment definitions and records their roles in interpreting the residual.

Fields — `status`, `headline_question`, `estimation_path`, `candidate_objects`, `recommendation`, `takeaways`.

### `output/models/split_treatment_architecture_summary.json`

Separates TDC into a deposit-proximate core and a TOC/ROW support component. Contrasts their quarter-by-quarter alignment and their responses at selected horizons.

Fields — `status`, `headline_question`, `estimation_path`, `series_definitions`, `quarterly_alignment`, `architecture_recommendation`, `key_horizons`, `takeaways`.

### `output/models/core_treatment_promotion_summary.json`

Checks the core series definition, prediction quality and residual responses under a separately estimated core shock. Records the resulting classification and strict-validation check.

Fields — `status`, `headline_question`, `estimation_path`, `series_alias_check`, `shock_quality`, `key_horizons`, `strict_validation_check`, `promotion_recommendation`, `takeaways`.

### `output/models/strict_redesign_summary.json`

Collects observed failure modes of the strict source decomposition under the split treatment. The output includes its diagnostic ordering in `recommended_build_order`.

Fields — `status`, `headline_question`, `estimation_path`, `current_strict_problem_definition`, `failure_modes`, `recommended_build_order`, `takeaways`.

### `output/models/strict_loan_core_redesign_summary.json`

Constructs alternative direct loan cores. Compares mortgages plus consumer credit with broader loan subtotals and the loans-not-elsewhere-classified component at common horizons.

Fields — `status`, `headline_question`, `estimation_path`, `candidate_definitions`, `published_roles`, `recommendation`, `key_horizons`, `takeaways`.

### `output/models/strict_di_bucket_role_summary.json`

Compares the broad DI loan bucket with narrower private-borrower components against the non-TDC residual. Assigns the bucket a role based on the reported comparisons.

Fields — `status`, `headline_question`, `estimation_path`, `release_taxonomy`, `recommendation`, `key_horizons`, `takeaways`.

### `output/models/strict_di_bucket_bridge_summary.json`

Links the lender-side DI loan asset to systemwide borrower liabilities. Reports borrower contributions and the remaining difference across the two perimeters.

Fields — `status`, `headline_question`, `estimation_path`, `bridge_definitions`, `recommendation`, `key_horizons`, `takeaways`.

### `output/models/strict_private_borrower_bridge_summary.json`

Adds private domestic borrower liabilities to the minimum loan core and compares that series with the core alone and broader DI-loan measures.

Fields — `status`, `headline_question`, `estimation_path`, `bridge_definitions`, `recommendation`, `key_horizons`, `takeaways`.

### `output/models/strict_nonfinancial_corporate_bridge_summary.json`

Uses nonfinancial corporate borrowers as a narrower addition to the loan core. Compares this series with the broader private-borrower addition.

Fields — `status`, `headline_question`, `estimation_path`, `bridge_definitions`, `recommendation`, `key_horizons`, `takeaways`.

### `output/models/strict_private_offset_residual_summary.json`

Isolates the household/nonprofit and nonfinancial noncorporate portions of the private-borrower addition. Measures the size and direction of their combined offset.

Fields — `status`, `headline_question`, `estimation_path`, `bridge_definitions`, `recommendation`, `key_horizons`, `takeaways`.

### `output/models/strict_corporate_bridge_secondary_comparison_summary.json`

Holds the direct loan core fixed and compares adding all private borrowers (`strict_loan_core_plus_private_borrower_qoq`) with adding nonfinancial corporations alone (`strict_loan_core_plus_nonfinancial_corporate_qoq`).

Fields — `status`, `headline_question`, `estimation_path`, `candidate_definitions`, `recommendation`, `key_horizons`, `takeaways`.

### `output/models/strict_component_framework_summary.json`

Combines accounting closure, broad TDC, TOC/ROW support and loan-core comparisons. Classifies which measures are arithmetic reconstructions, direct observations or cross-perimeter comparisons.

Fields — `status`, `headline_question`, `estimation_path`, `frozen_roles`, `h0_snapshot`, `classification`, `recommendation`, `takeaways`.

### `output/models/strict_release_framing_summary.json`

Combines TOC/ROW incidence diagnostics with the direct loan-core estimates. Records evidence tiers, the strict perimeter and the conditions for assigning a measured leg to that perimeter.

Fields — `status`, `headline_question`, `estimation_path`, `release_position`, `evidence_tiers`, `classification`, `h0_snapshot`, `recommendation`, `takeaways`.

### `output/models/strict_direct_core_component_summary.json`

Separates the mortgage and consumer-credit contributions to the bundled loan core and compares each with the same deposit-proximate residual.

Fields — `status`, `headline_question`, `estimation_path`, `candidate_definitions`, `key_horizons`, `classification`, `recommendation`, `takeaways`.

### `output/models/strict_direct_core_horizon_stability_summary.json`

Ranks the component candidates by horizon. Compares the closest candidate at impact with the closest at four and eight quarters.

Fields — `status`, `headline_question`, `estimation_path`, `horizon_winners`, `classification`, `recommendation`, `takeaways`.

### `output/models/strict_additional_creator_candidate_summary.json`

Compares other candidate credit channels with the mortgage and consumer-credit core. Groups them by direct measurement versus proxy coverage.

Fields — `status`, `headline_question`, `estimation_path`, `candidate_groups`, `key_horizons`, `classification`, `recommendation`, `takeaways`.

### `output/models/strict_di_loans_nec_measurement_audit_summary.json`

Evaluates whether borrower-sector measures can identify the DI loans-not-elsewhere-classified component at the same institutional perimeter as the lender measure.

Fields — `status`, `headline_question`, `estimation_path`, `candidate_groups`, `classification`, `recommendation`, `key_horizons`, `takeaways`.

### `output/models/strict_results_closeout_summary.json`

Consolidates direct-core and bridge comparisons, evidence tiers, remaining identification questions and the scope of the strict decomposition.

Fields — `status`, `headline_question`, `estimation_path`, `release_position`, `settled_findings`, `evidence_tiers`, `unresolved_questions`, `classification`, `recommendation`, `takeaways`.

### `output/models/treasury_cash_regime_audit_summary.json`

Compares the operating-cash term with the TGA alone and with a cash-balance measure that includes bank-side federal balances. Reports full-sample and regime-specific results.

Fields — `status`, `headline_question`, `estimation_path`, `definitions`, `regime_windows`, `full_sample`, `classification`, `recommendation`, `takeaways`.

### `output/models/historical_cash_term_reestimation_summary.json`

Recalculates the 1990Q1–2002Q3 backfill with a Treasury Tax and Loan era cash proxy. Compares historical windows and identifies quarters with the largest adjustments.

Fields — `status`, `headline_question`, `estimation_path`, `definitions`, `windows`, `comparison`, `top_adjustment_quarters`, `classification`, `recommendation`, `takeaways`.

### `output/models/toc_row_incidence_audit_summary.json`

Assesses TOC and ROW separately against the bank-deposit liability perimeter using quarterly alignments and selected shock responses.

Fields — `status`, `headline_question`, `estimation_path`, `leg_definitions`, `quarterly_alignment`, `key_horizons`, `classification`, `recommendation`, `takeaways`.

### `output/models/toc_row_liability_incidence_raw_summary.json`

Pairs each leg with observed bank liabilities in original monetary units in the same and following quarter. Compares those mappings with impact and one-quarter shock responses.

Fields — `status`, `headline_question`, `estimation_path`, `leg_definitions`, `quarterly_alignment`, `key_horizons`, `classification`, `recommendation`, `takeaways`.

### `output/models/toc_validated_share_candidate_summary.json`

Screens narrower TOC allocations for quarterly stability and compares surviving candidates with the direct-core benchmark.

Fields — `status`, `headline_question`, `estimation_path`, `candidate_definitions`, `quarterly_gate`, `key_horizons`, `classification`, `recommendation`, `takeaways`.

### `output/models/tdc_treatment_audit_summary.json`

Reconstructs TDC from its public components, checks its quarter-by-quarter agreement with the imported series, and compares responses when individual legs are omitted.

Fields — `status`, `headline_question`, `estimation_path`, `component_definitions`, `variant_definitions`, `baseline_target`, `construction_alignment`, `key_horizons`, `takeaways`.

### `output/models/identity_measurement_ladder.csv`

Repeats the common-sample identity decomposition for alternative upstream TDC measurements. Derives the non-TDC response as total deposits minus the corresponding TDC response.

Columns — `treatment_variant`, `treatment_role`, `treatment_family`, `target`, `outcome`, `horizon`, `beta`, `se`, `lower95`, `upper95`, `n`, `spec_name`, `shock_column`, `decomposition_mode`, `outcome_construction`, `inference_method`.

### `output/models/identity_treatment_sensitivity.csv`

Repeats the exact identity decomposition under alternative treatment definitions using a common design within each variant.

Columns — `treatment_variant`, `treatment_role`, `treatment_family`, `target`, `outcome`, `horizon`, `beta`, `se`, `lower95`, `upper95`, `n`, `spec_name`, `shock_column`, `decomposition_mode`, `outcome_construction`, `inference_method`.

### `output/models/identity_control_sensitivity.csv`

Repeats the exact identity decomposition with alternative control sets while keeping the regressors common to the component equations.

Columns — `control_variant`, `control_role`, `control_columns`, `outcome`, `horizon`, `beta`, `se`, `lower95`, `upper95`, `n`, `spec_name`, `shock_column`, `shock_scale`, `response_type`, `decomposition_mode`, `outcome_construction`, `inference_method`.

### `output/models/identity_sample_sensitivity.csv`

Repeats the exact identity decomposition on the specified shock samples and compares the estimated component responses.

Columns — `sample_variant`, `sample_role`, `sample_filter`, `outcome`, `horizon`, `beta`, `se`, `lower95`, `upper95`, `n`, `spec_name`, `shock_column`, `shock_scale`, `response_type`, `decomposition_mode`, `outcome_construction`, `inference_method`.

### `output/models/lp_irf_regimes.csv`

Estimates local projections in groups defined by the configured regime splits. The baseline liquidity-pressure split uses the negative lagged change in reserves.

Columns — `regime`, `outcome`, `horizon`, `beta`, `se`, `lower95`, `upper95`, `n`, `spec_name`.

### `output/models/regime_diagnostics_summary.json`

Summarizes observations and within-group shock support for the regime splits. Reports the number classified as informative and stable.

Fields — `informative_regime_count`, `stable_regime_count`, `regimes`, `takeaways`.

### `output/models/tdc_sensitivity_ladder.csv`

Estimates responses under alternative measurement and shock specifications. The treatment role and family identify how each variant relates to the baseline.

Columns — `treatment_variant`, `treatment_role`, `treatment_family`, `outcome`, `horizon`, `beta`, `se`, `lower95`, `upper95`, `n`, `spec_name`.

### `output/models/control_set_sensitivity.csv`

Refits local projections for the same shock with different controls. The role distinguishes the headline lagged-macro specification, nearby `core` control sets and `exploratory` timing checks.

Columns — `control_variant`, `control_role`, `control_columns`, `outcome`, `horizon`, `beta`, `se`, `lower95`, `upper95`, `n`, `spec_name`.

### `output/models/shock_sample_sensitivity.csv`

Refits local projections on the full usable shock sample and filtered samples, including the exclusion of numerically flagged fits.

Columns — `sample_variant`, `sample_role`, `sample_filter`, `outcome`, `horizon`, `beta`, `se`, `lower95`, `upper95`, `n`, `spec_name`.

### `output/models/period_sensitivity.csv`

Refits the baseline local projections within the specified date windows. The `all_usable` window spans the full usable sample. The `post_gfc_early`, `pre_covid` and `covid_post` windows restrict estimation to their specified quarters.

Columns — `period_variant`, `period_role`, `start_quarter`, `end_quarter`, `outcome`, `horizon`, `beta`, `se`, `lower95`, `upper95`, `n`, `spec_name`.

### `output/models/period_sensitivity_summary.json`

Selects headline horizons from the period-specific responses and compares deposit and residual estimates with their confidence intervals. The `crowd_out_signal` label requires a positive total-deposit response and a negative other-component response with both intervals excluding zero.

Fields — `status`, `headline_question`, `estimation_path`, `periods`, `key_horizons`, `takeaways`.

### `output/models/total_minus_other_contrast.csv`

Subtracts the estimated other-component response from the total-deposit response and compares it with the directly estimated TDC response under the same specification.

Columns — `scope`, `variant`, `role`, `horizon`, `beta_total`, `beta_other`, `beta_implied`, `beta_direct`, `gap_implied_minus_direct`, `abs_gap`, `n_total`, `n_other`, `n_direct`, `sample_mismatch_flag`.

The direct estimate can also carry `direct_lower95`, `direct_upper95` and `direct_ci_excludes_zero`. The `identity_check_mode` labels the comparison and `contrast_consistent` records agreement within tolerance on matched counts.

### `output/models/structural_proxy_evidence.csv`

Pairs the residual response with each structural proxy at the same horizon. Reports signs, confidence intervals and the signed ratio of the proxy coefficient to the residual coefficient.

Columns — `scope`, `context`, `horizon`, `other_outcome`, `other_beta`, `other_se`, `other_lower95`, `other_upper95`, `other_ci_excludes_zero`, `proxy_outcome`, `proxy_beta`, `proxy_se`, `proxy_lower95`, `proxy_upper95`, `proxy_ci_excludes_zero`, `other_sign`, `proxy_sign`, `sign_alignment`, `evidence_label`, `proxy_share_of_other_beta`.

### `output/models/structural_proxy_evidence_summary.json`

Summarizes the direction and uncertainty of proxy and residual responses at selected horizons.

Fields — `status`, `headline_question`, `key_horizons`, `takeaways`.

### `output/models/proxy_coverage_summary.json`

Aggregates proxy comparisons by channel family. Records measured and uncovered families, differences in historical coverage, and comparisons by horizon and regime.

Fields — `status`, `headline_question`, `covered_channel_families`, `major_uncovered_channel_families`, `history_limits`, `key_horizons`, `published_regime_contexts`, `release_caveat`, `takeaways`.

### `output/models/call_report_deposit_components_summary.json`

Checks normalized Call Report rows for quarter format, duplicates, negative or missing balances and institutional scope. Reports zero rows when the source is unavailable.

Fields — `status`, `row_count`, `qa`, `takeaways`.

### `output/models/proxy_unit_audit.json`

Records the input series and divisors used to convert FRED balance-sheet series into billions of US dollars, together with the resulting derived proxy definitions.

Fields — `status`, `source_series`, `derived_proxies`, `takeaways`.

### `output/models/shock_diagnostics_summary.json`

Compares shock scale and usable quarters across variants. Summarizes impact responses, forecast quality and quarters where treatment estimates differ most.

Fields — `estimand_interpretation`, `sample_comparison`, `impact_response_comparison`, `treatment_variant_comparisons`, `largest_disagreement_quarters`, `takeaways`.

### `output/models/headline_treatment_fingerprint.json`

Records the shock target, predictors, fitted method, training limits and usable sample along with the producer commit, tree state, input identity and configuration hashes.

Fields — `treatment_freeze_status`, `model_name`, `target`, `method`, `predictors`, `min_train_obs`, `max_train_obs`, `usable_sample`, `analysis_source_commit`, `analysis_tree`, `config_hashes`, `upstream_input`.

### `output/models/provenance_validation_summary.json`

Checks the recorded producer, build tree, configuration hashes, shock metadata and upstream input against the treatment fingerprint. Lists failed checks.

Fields — `status`, `failures`, `analysis_source_commit_check`, `analysis_tree_check`, `config_hashes_check`, `upstream_input_check`, `spec_metadata_check`.

### `output/models/direct_identification_summary.json`

Checks whether the shock moves TDC itself and whether total and other deposit responses separate at the reported horizons. Compares the full and trimmed samples.

Fields — `status`, `headline_question`, `estimation_path`, `shock_definition`, `horizon_evidence`, `first_stage_checks`, `sample_fragility`, `answer_ready`, `reasons`, `warnings`, `answer_ready_when`.

### `output/models/result_readiness_summary.json`

Combines identification, sample and mechanism diagnostics into an assessment of which deposit-response interpretations the estimates support. Lists reasons and warnings.

Fields — `status`, `estimation_path`, `headline_assessment`, `reasons`, `warnings`, `diagnostics`, `key_estimates`, `counterpart_channel_context`, `answer_ready_when`.

### `output/models/pass_through_summary.json`

Joins the deposit, residual and direct TDC responses with control, treatment and sample comparisons. Each named context contains results from the corresponding diagnostic. The readiness fields describe the limits of the resulting interpretation.

Fields — `status`, `headline_question`, `headline_answer`, `estimation_path`, `sample_policy`, `baseline_horizons`, `core_treatment_variants`, `measurement_treatment_variants`, `shock_design_treatment_variants`, `core_control_variants`, `shock_sample_variants`, `structural_proxy_context`, `proxy_coverage_context`, `counterpart_channel_context`, `scope_alignment_context`, `strict_gap_scope_check_context`, `broad_scope_system_context`, `tdc_treatment_audit_context`, `treasury_operating_cash_audit_context`, `rest_of_world_treasury_audit_context`, `toc_row_path_split_context`, `toc_row_excluded_interpretation_context`, `strict_missing_channel_context`, `strict_sign_mismatch_audit_context`, `strict_shock_composition_context`, `strict_top_gap_quarter_audit_context`, `strict_top_gap_quarter_direction_context`, `strict_top_gap_inversion_context`, `strict_top_gap_anomaly_context`, `strict_top_gap_anomaly_component_split_context`, `strict_top_gap_anomaly_di_loans_split_context`, `strict_top_gap_anomaly_backdrop_context`, `big_picture_synthesis_context`, `treatment_object_comparison_context`, `split_treatment_architecture_context`, `core_treatment_promotion_context`, `strict_redesign_context`, `strict_loan_core_redesign_context`, `strict_di_bucket_role_context`, `strict_di_bucket_bridge_context`, `strict_private_borrower_bridge_context`, `strict_nonfinancial_corporate_bridge_context`, `strict_private_offset_residual_context`, `strict_corporate_bridge_secondary_comparison_context`, `strict_component_framework_context`, `strict_release_framing_context`, `strict_direct_core_component_context`, `strict_direct_core_horizon_stability_context`, `strict_additional_creator_candidate_context`, `toc_row_incidence_audit_context`, `tdcest_ladder_integration_context`, `tdcest_broad_object_comparison_context`, `tdcest_broad_treatment_sensitivity_context`, `published_regime_contexts`, `readiness_reasons`, `readiness_warnings`.

The optional `toc_validated_share_candidate_context` holds the TOC share stability and direct-core comparison. The `strict_gap_scope_check_context` applies the matched-perimeter residual shift while holding the strict total fixed.

### `output/models/tdcest_ladder_integration_summary.json`

Assigns roles to the upstream `tdcest` bank-only series, corrected measurement series, historical bank-receipt overlay and ROW pilot in the downstream comparison.

Fields — `status`, `headline_question`, `estimation_path`, `classification`, `series_roles`, `recommendation`, `takeaways`.

### `output/models/tdcest_broad_object_comparison_summary.json`

Aligns the broad series on their latest common quarter and records coverage of the historical overlay and ROW pilot.

Fields — `status`, `headline_question`, `estimation_path`, `latest_common_broad_comparison`, `supplemental_surfaces`, `classification`, `recommendation`, `takeaways`.

### `output/models/tdcest_broad_treatment_sensitivity_summary.json`

Estimates deposit and residual responses to the canonical broad shock and corrected Tier 2 and Tier 3 shocks. Compares their signs and magnitudes.

Fields — `status`, `headline_question`, `estimation_path`, `classification`, `key_horizons`, `recommendation`, `takeaways`.

### `output/models/deposit_component_scorecard.json`

Compares deposit-component outcomes and candidate credit-creation channels at the same local-projection horizons.

Fields — `status`, `headline_question`, `estimation_path`, `component_outcomes_present`, `creator_channel_outcomes_present`, `horizons`, `takeaways`.

### `output/models/deposit_type_side_read.csv`

Selects deposit-type response rows and assigns sign and interval classifications to each outcome and horizon.

Columns — `outcome`, `display_name`, `horizon`, `horizon_label`, `beta`, `se`, `lower95`, `upper95`, `n`, `ci_excludes_zero`, `sign_label`, `interpretation_note`.

### `output/models/counterpart_channel_scorecard.json`

Compares residual responses with measured credit-creation and funding or accommodation outcomes by horizon. Identifies which channels are present.

Fields — `status`, `headline_question`, `estimation_path`, `legacy_private_credit_proxy_role`, `creator_channel_outcomes_present`, `funding_accommodation_outcomes_present`, `horizons`, `takeaways`.

### `output/models/sample_construction_summary.json`

Counts quarters before and after the baseline completeness rule, explains sample truncation and records proxy coverage and usable shocks after training. Optional `interpretation_scope` and `mechanism_caveat` describe the supported interpretation.

Fields — `full_panel`, `headline_sample`, `usable_shock_sample`, `shock_definition`, `headline_sample_truncation`, `extended_column_coverage`, `takeaways`.

### `output/manifests/raw_downloads.json`

Records the raw acquisition runs used to assemble the inputs. The `runs` collection identifies the download records.

Fields — `runs`.

### `output/manifests/reused_artifacts.json`

Records input artifacts obtained from existing local sources. The `artifacts` collection identifies the reused inputs.

Fields — `artifacts`.

### `output/manifests/pipeline_run.json`

Records the invoked pipeline command and the paths it wrote in `outputs`.

Fields — `command`, `outputs`.

### `site/data/overview.json`

Combines accounting metrics, sample span and counts, findings, caveats, evidence tiers and artifact references for the local site export.

Fields — `headline_metrics`, `sample`, `main_findings`, `caveats`, `evidence_tiers`, `artifacts`.

## Shared CSV columns

Monetary response coefficients and their uncertainty use billions of US dollars per unit of shock. For the standardized baseline, that unit is one training residual standard deviation. The cumulative specification sums quarterly outcomes from impact through the named horizon. A ratio is dimensionless. A missing estimate or an undefined ratio has no numerical interpretation.

| Fields | Meaning and units |
| --- | --- |
| `metric`, `value`, `notes` | Statistic name, its numerical value and its definition. Means use the input series units. Shares and correlations are dimensionless. |
| `quarter`, `source_quarter` | Calendar quarter in YYYYQn format and the originating reporting quarter. |
| `account_type`, `depositor_class`, `universe_basis` | Deposit account category, holder category and the set of institutions covered. |
| `amount_bil_usd`, `institution_count` | Reported balance in billions of US dollars and the count of reporting institutions. |
| `source_kind` | Type of input or measurement used to build the record. |
| `model_name`, `target` | Model identifier and series it predicts. |
| `tdc_fitted`, `tdc_residual`, `tdc_residual_z` | Out-of-sample TDC prediction, observed TDC minus prediction (both in billions of US dollars), and residual divided by the training residual standard deviation. |
| `train_start_obs` | Number of complete observations used to fit that quarter’s model. |
| `train_condition_number` | Condition number of the training design after scaling nonconstant predictors. Dimensionless. |
| `train_target_sd`, `train_resid_sd` | Training standard deviations of the target and model residual, in target units. |
| `fitted_to_target_scale_ratio`, `fitted_to_train_target_sd_ratio` | Absolute fitted value divided by the absolute realized target (with a small denominator floor), or by the training target standard deviation. Both are ratios. |
| `shock_flag` | Pipe-separated numerical warnings for the fitted quarter. An empty string means no threshold was flagged. |
| `outcome`, `display_name`, `other_outcome`, `proxy_outcome` | Series identifier, readable series label, or identifiers for the compared residual and proxy. |
| `horizon`, `horizon_label` | Quarters after the shock and the corresponding text label. Zero denotes the shock quarter. |
| `beta`, `se`, `lower95`, `upper95` | Estimated response to a one-unit shock, its standard error and the lower and upper 95% confidence limits. All are in outcome units per unit of the specified shock. |
| `n` | Number of usable observations for this equation and horizon. |
| `spec_name`, `shock_column`, `shock_scale` | Specification identifier, shock series name and units of the shock (a rolling out-of-sample standard deviation or a raw unit). |
| `response_type` | Whether the reported response sums outcomes from impact through the horizon or uses only the outcome at the horizon. |
| `decomposition_mode`, `outcome_construction`, `inference_method` | Method used to enforce the accounting identity, how each outcome response was constructed, and method used to estimate its uncertainty. |
| `regime`, `scope`, `context` | Regime split, specification subset or comparison in which the row was computed. |
| `variant`, `treatment_variant`, `control_variant`, `sample_variant`, `period_variant` | Name of the alternative being compared. The prefix identifies the input or sample that changes. |
| `role`, `treatment_role`, `control_role`, `sample_role`, `period_role` | Role in the comparison, such as headline, core sensitivity or exploratory. |
| `treatment_family` | Whether the variant changes the measurement of TDC or the method for estimating the shock. |
| `control_columns`, `sample_filter` | Controls included in the regression and the rule selecting observations. |
| `start_quarter`, `end_quarter` | Calendar boundaries of the period variant. |
| `beta_total`, `beta_other`, `beta_direct`, `beta_implied` | Total-deposit, other-component and directly fitted TDC responses, plus the implied TDC response computed as total minus other. Units match the component estimates. |
| `gap_implied_minus_direct`, `abs_gap` | Implied minus direct TDC response and its absolute magnitude, in response units. |
| `n_total`, `n_other`, `n_direct`, `sample_mismatch_flag` | Observations in the total, other and direct fits, and an indicator of whether the compared counts differ. |
| `residual_beta`, `accounting_total_beta`, `identity_gap_beta` | Responses of the non-TDC residual, the reconstructed accounting total and the accounting gap, respectively. |
| `strict_loan_source_beta`, `strict_non_treasury_securities_beta`, `strict_identifiable_total_beta`, `strict_identifiable_gap_beta` | Responses of loan sources, non-Treasury securities, their gross identifiable total and the uncovered residual. |
| `arithmetic_residual_minus_total_beta` | Residual coefficient minus the reconstructed or identifiable total coefficient in that table. |
| `identity_gap_share_of_residual`, `strict_gap_share_of_residual` | Absolute accounting-gap or strict-gap response divided by the absolute residual response. |
| `strict_funding_offset_total_beta`, `strict_identifiable_net_after_funding_beta`, `strict_gap_after_funding_beta` | Responses of measured funding offsets, sources net of funding and the remaining residual after funding. |
| `strict_funding_offset_share_of_identifiable_total_beta` | Funding-offset response divided by the gross identifiable-total response. |
| `residual_n`, `accounting_total_n`, `identity_gap_n`, `strict_total_n`, `strict_gap_n`, `identifiable_total_n`, `funding_total_n`, `net_after_funding_n`, `gap_after_funding_n` | Observation count for the named response in the alignment table. |
| `other_beta`, `other_se`, `other_lower95`, `other_upper95`, `proxy_beta`, `proxy_se`, `proxy_lower95`, `proxy_upper95` | Coefficient, standard error and 95% interval for the non-TDC or proxy equation, in the corresponding response units. |
| `other_sign`, `proxy_sign`, `sign_alignment`, `sign_label` | Direction of each response or agreement between the compared directions. |
| `other_ci_excludes_zero`, `proxy_ci_excludes_zero`, `ci_excludes_zero` | Boolean indicating whether the specified 95% interval lies entirely above or below zero. |
| `proxy_share_of_other_beta` | Signed proxy response divided by the other-component response. |
| `evidence_label`, `interpretation`, `interpretation_note` | Categorical evidence assessment or text explaining the sign, coverage or uncertainty of the comparison. |

## JSON fields

Each list above names the top-level fields of its JSON object. Nested horizon records use the response, uncertainty and count conventions of the CSV tables. Objects ending in `_context` summarize the corresponding diagnostic in the file list. Labels, identifiers, descriptions and booleans have no physical unit.

| Fields | Contents |
| --- | --- |
| `status` | Outcome or availability of the diagnostic. A status has meaning within its own diagnostic. |
| `headline_question`, `headline_answer`, `headline_assessment` | Question evaluated by the summary, its reported answer, or the assessment attached to that answer. |
| `estimation_path`, `source_kind`, `strict_source_kind` | Estimation method or origin of the inputs used for the summary. |
| `horizons`, `key_horizons`, `baseline_horizons`, `horizon_evidence`, `key_estimates`, `h0_snapshot` | Estimates and diagnostics organized by response horizon. An h0 snapshot contains impact-quarter results. |
| `recommendation`, `promotion_recommendation`, `architecture_recommendation`, `recommended_build_order` | Classification, preferred comparison or ordered diagnostic checks returned by the producer. |
| `interpretation`, `takeaways`, `classification`, `evidence_tiers`, `release_taxonomy`, `release_position`, `frozen_roles`, `published_roles`, `series_roles` | Interpretation text and labels assigning an evidentiary role or level of support to each result. |
| `definitions`, `candidate_definitions`, `comparison_definition`, `bridge_definitions`, `series_definitions`, `component_definitions`, `leg_definitions`, `path_definitions`, `variant_definitions` | Names and constructions of the inputs, candidates or comparison paths in that diagnostic. |
| `candidate_objects`, `candidate_groups`, `deposit_concepts`, `component_outcomes_present`, `creator_channel_outcomes_present`, `funding_accommodation_outcomes_present` | Candidates examined, deposit perimeters used or channels with available response rows. |
| `quarterly_alignment`, `construction_alignment`, `shock_alignment`, `component_alignment`, `gap_driver_alignment`, `quarterly_split`, `quarterly_gate` | Quarter-by-quarter comparisons of the named series, decomposition of their joint movement, or result of screening the proposed allocation. |
| `top_gap_quarters`, `largest_disagreement_quarters`, `top_adjustment_quarters`, `quarter_concentration`, `quarter_composition` | Ranked quarters and contributions to disagreement, adjustment or composition. Quarter identifiers use YYYYQn. |
| `periods`, `windows`, `regime_windows`, `regimes`, `period_bucket_profiles` | Sample boundaries and results summarized within each calendar period or regime. |
| `anomaly_quarter`, `peer_quarters`, `peer_bucket_weight`, `weighted_peer_means`, `anomaly_vs_peer_deltas` | Selected quarter, comparison quarters, their aggregate weight, weighted peer averages and anomaly-minus-peer differences. |
| `loan_subcomponent_deltas`, `securities_subcomponent_deltas`, `funding_subcomponent_deltas`, `liquidity_external_deltas`, `di_loans_nec_component_deltas`, `ranked_anomaly_component_deltas`, `ranked_component_deltas` | Anomaly-minus-peer differences for the named component group, or those differences ranked by size. Differences retain the component units. |
| `backdrop_rows`, `corporate_credit_row`, `loan_source_row`, `reserves_row`, `foreign_nonts_row`, `tga_row`, `residual_row`, `borrower_gap_row` | Quarter-level comparison records for the named credit, liquidity, external or residual series. |
| `dominant_borrower_component`, `liquidity_external_abs_to_corporate_abs_ratio` | Largest borrower-side contributor and ratio of absolute liquidity/external differences to the absolute corporate difference. |
| `dominant_leg_summary`, `contribution_pattern_summary`, `gap_bundle_alignment_summary`, `directional_driver_summary`, `directional_driver_context_summary`, `residual_strict_pattern_summary`, `peer_pattern_summary` | Counts or summaries of dominant legs, sign combinations and directional patterns among the selected quarters. |
| `full_panel`, `headline_sample`, `usable_shock_sample`, `usable_sample`, `full_sample`, `sample`, `headline_sample_truncation`, `extended_column_coverage` | Quarter spans, counts, exclusions or series coverage at successive stages of panel construction or estimation. |
| `sample_policy`, `shock_definition`, `predictors`, `model_name`, `target`, `method`, `min_train_obs`, `max_train_obs` | Rules defining the sample and shock, regressor names, method and lower/upper bounds on the number of training observations. |
| `treatment_freeze_status`, `analysis_source_commit`, `analysis_tree`, `config_hashes`, `upstream_input` | Specification status, recorded Git revision, working tree state, configuration digests and upstream input identity. |
| `analysis_source_commit_check`, `analysis_tree_check`, `config_hashes_check`, `upstream_input_check`, `spec_metadata_check`, `series_alias_check`, `strict_validation_check`, `failures` | Individual comparisons against recorded inputs or definitions, the strict validation result, and checks that failed. |
| `horizon_winners` | Candidate closest to the residual response at each reported horizon. |
| `covered_channel_families`, `major_uncovered_channel_families`, `history_limits`, `published_regime_contexts`, `release_caveat` | Proxy coverage, omitted channel families, historical limitations, regime-specific results and interpretation limits. |
| `core_treatment_variants`, `measurement_treatment_variants`, `shock_design_treatment_variants`, `core_control_variants`, `shock_sample_variants`, `treatment_variant_comparisons`, `measurement_variant_comparison` | Response comparisons grouped by treatment definition, shock specification, controls or sample. |
| `comparison`, `sample_comparison`, `impact_response_comparison`, `latest_common_broad_comparison`, `supplemental_surfaces` | Comparisons across estimates, usable samples, impact responses or broad series over a common quarter, and references to supplementary measurements. |
| `first_stage_checks`, `shock_quality`, `sample_fragility`, `trim_diagnostics`, `diagnostics`, `qa` | Checks on direct TDC response, forecast quality, sensitivity to sample restrictions or input validity. The diagnostic determines the measures reported. |
| `informative_regime_count`, `stable_regime_count`, `row_count` | Number of informative regimes, stable regimes, or normalized input rows. |
| `source_series`, `derived_proxies` | Original source series with scale factors, and proxy channels derived from them. |
| `strict_component_diagnostics`, `di_loans_nec_borrower_diagnostics`, `funding_offset_sensitivity`, `broad_matched_system`, `tdc_component_audit` | Component and borrower diagnostics, effect of netting measured funding, results for the broad matched perimeter or reconstruction of TDC. |
| `current_strict_problem_definition`, `failure_modes`, `supporting_case`, `settled_findings`, `unresolved_questions` | Documented scope of the strict measurement, observed failure patterns, supporting quarter-level evidence and remaining uncertainty. |
| `estimand_interpretation`, `legacy_private_credit_proxy_role`, `baseline_target` | Meaning of the estimated response, the role of the private-credit proxy, or the baseline series being predicted. |
| `answer_ready`, `answer_ready_when`, `reasons`, `warnings`, `readiness_reasons`, `readiness_warnings` | Readiness indicator, conditions for interpretation, supporting reasons and warnings. |
| `headline_metrics`, `main_findings`, `caveats`, `artifacts` | Overview quantities, narrative findings, qualifications and artifact references. In the reuse manifest, artifacts are reused source records. |
| `runs`, `command`, `outputs` | Raw download records, invoked pipeline command and paths written by a run. |

## Local site copies

The pipeline exports the following files for the local site. Each entry links to its source schema above. Units and field meanings carry through to the copy.

### `site/data/accounting_summary.csv`

Copy of [`output/accounting/accounting_summary.csv`](#outputaccountingaccounting_summarycsv).

### `site/data/quarters_tdc_exceeds_total.csv`

Copy of [`output/accounting/quarters_tdc_exceeds_total.csv`](#outputaccountingquarters_tdc_exceeds_totalcsv).

### `site/data/unexpected_tdc.csv`

Copy of [`output/shocks/unexpected_tdc.csv`](#outputshocksunexpected_tdccsv). The site contract omits `fitted_to_train_target_sd_ratio` from its minimum columns.

### `site/data/lp_irf.csv`

Copy of [`output/models/lp_irf.csv`](#outputmodelslp_irfcsv).

### `site/data/lp_irf_identity_baseline.csv`

Copy of [`output/models/lp_irf_identity_baseline.csv`](#outputmodelslp_irf_identity_baselinecsv).

### `site/data/lp_irf_accounting_identity.csv`

Copy of [`output/models/lp_irf_accounting_identity.csv`](#outputmodelslp_irf_accounting_identitycsv).

### `site/data/lp_irf_strict_identifiable.csv`

Copy of [`output/models/lp_irf_strict_identifiable.csv`](#outputmodelslp_irf_strict_identifiablecsv).

### `site/data/accounting_identity_alignment.csv`

Copy of [`output/models/accounting_identity_alignment.csv`](#outputmodelsaccounting_identity_alignmentcsv).

### `site/data/strict_identifiable_alignment.csv`

Copy of [`output/models/strict_identifiable_alignment.csv`](#outputmodelsstrict_identifiable_alignmentcsv).

### `site/data/strict_funding_offset_alignment.csv`

Copy of [`output/models/strict_funding_offset_alignment.csv`](#outputmodelsstrict_funding_offset_alignmentcsv).

### `site/data/accounting_identity_summary.json`

Copy of [`output/models/accounting_identity_summary.json`](#outputmodelsaccounting_identity_summaryjson).

### `site/data/strict_identifiable_summary.json`

Copy of [`output/models/strict_identifiable_summary.json`](#outputmodelsstrict_identifiable_summaryjson).

### `site/data/strict_identifiable_followup_summary.json`

Copy of [`output/models/strict_identifiable_followup_summary.json`](#outputmodelsstrict_identifiable_followup_summaryjson).

### `site/data/scope_alignment_summary.json`

Copy of [`output/models/scope_alignment_summary.json`](#outputmodelsscope_alignment_summaryjson).

### `site/data/broad_scope_system_summary.json`

Copy of [`output/models/broad_scope_system_summary.json`](#outputmodelsbroad_scope_system_summaryjson).

### `site/data/treasury_operating_cash_audit_summary.json`

Copy of [`output/models/treasury_operating_cash_audit_summary.json`](#outputmodelstreasury_operating_cash_audit_summaryjson).

### `site/data/rest_of_world_treasury_audit_summary.json`

Copy of [`output/models/rest_of_world_treasury_audit_summary.json`](#outputmodelsrest_of_world_treasury_audit_summaryjson).

### `site/data/toc_row_bundle_audit_summary.json`

Copy of [`output/models/toc_row_bundle_audit_summary.json`](#outputmodelstoc_row_bundle_audit_summaryjson).

### `site/data/toc_row_path_split_summary.json`

Copy of [`output/models/toc_row_path_split_summary.json`](#outputmodelstoc_row_path_split_summaryjson).

### `site/data/toc_row_excluded_interpretation_summary.json`

Copy of [`output/models/toc_row_excluded_interpretation_summary.json`](#outputmodelstoc_row_excluded_interpretation_summaryjson).

### `site/data/strict_missing_channel_summary.json`

Copy of [`output/models/strict_missing_channel_summary.json`](#outputmodelsstrict_missing_channel_summaryjson).

### `site/data/strict_sign_mismatch_audit_summary.json`

Copy of [`output/models/strict_sign_mismatch_audit_summary.json`](#outputmodelsstrict_sign_mismatch_audit_summaryjson).

### `site/data/strict_shock_composition_summary.json`

Copy of [`output/models/strict_shock_composition_summary.json`](#outputmodelsstrict_shock_composition_summaryjson).

### `site/data/strict_top_gap_quarter_audit_summary.json`

Copy of [`output/models/strict_top_gap_quarter_audit_summary.json`](#outputmodelsstrict_top_gap_quarter_audit_summaryjson).

### `site/data/strict_top_gap_quarter_direction_summary.json`

Copy of [`output/models/strict_top_gap_quarter_direction_summary.json`](#outputmodelsstrict_top_gap_quarter_direction_summaryjson).

### `site/data/strict_top_gap_inversion_summary.json`

Copy of [`output/models/strict_top_gap_inversion_summary.json`](#outputmodelsstrict_top_gap_inversion_summaryjson).

### `site/data/strict_top_gap_anomaly_summary.json`

Copy of [`output/models/strict_top_gap_anomaly_summary.json`](#outputmodelsstrict_top_gap_anomaly_summaryjson).

### `site/data/strict_top_gap_anomaly_component_split_summary.json`

Copy of [`output/models/strict_top_gap_anomaly_component_split_summary.json`](#outputmodelsstrict_top_gap_anomaly_component_split_summaryjson).

### `site/data/strict_top_gap_anomaly_di_loans_split_summary.json`

Copy of [`output/models/strict_top_gap_anomaly_di_loans_split_summary.json`](#outputmodelsstrict_top_gap_anomaly_di_loans_split_summaryjson).

### `site/data/strict_top_gap_anomaly_backdrop_summary.json`

Copy of [`output/models/strict_top_gap_anomaly_backdrop_summary.json`](#outputmodelsstrict_top_gap_anomaly_backdrop_summaryjson).

### `site/data/big_picture_synthesis_summary.json`

Copy of [`output/models/big_picture_synthesis_summary.json`](#outputmodelsbig_picture_synthesis_summaryjson).

### `site/data/treatment_object_comparison_summary.json`

Copy of [`output/models/treatment_object_comparison_summary.json`](#outputmodelstreatment_object_comparison_summaryjson).

### `site/data/split_treatment_architecture_summary.json`

Copy of [`output/models/split_treatment_architecture_summary.json`](#outputmodelssplit_treatment_architecture_summaryjson).

### `site/data/core_treatment_promotion_summary.json`

Copy of [`output/models/core_treatment_promotion_summary.json`](#outputmodelscore_treatment_promotion_summaryjson).

### `site/data/strict_redesign_summary.json`

Copy of [`output/models/strict_redesign_summary.json`](#outputmodelsstrict_redesign_summaryjson).

### `site/data/strict_loan_core_redesign_summary.json`

Copy of [`output/models/strict_loan_core_redesign_summary.json`](#outputmodelsstrict_loan_core_redesign_summaryjson).

### `site/data/strict_di_bucket_role_summary.json`

Copy of [`output/models/strict_di_bucket_role_summary.json`](#outputmodelsstrict_di_bucket_role_summaryjson).

### `site/data/strict_di_bucket_bridge_summary.json`

Copy of [`output/models/strict_di_bucket_bridge_summary.json`](#outputmodelsstrict_di_bucket_bridge_summaryjson).

### `site/data/strict_private_borrower_bridge_summary.json`

Copy of [`output/models/strict_private_borrower_bridge_summary.json`](#outputmodelsstrict_private_borrower_bridge_summaryjson).

### `site/data/strict_nonfinancial_corporate_bridge_summary.json`

Copy of [`output/models/strict_nonfinancial_corporate_bridge_summary.json`](#outputmodelsstrict_nonfinancial_corporate_bridge_summaryjson).

### `site/data/strict_private_offset_residual_summary.json`

Copy of [`output/models/strict_private_offset_residual_summary.json`](#outputmodelsstrict_private_offset_residual_summaryjson).

### `site/data/strict_corporate_bridge_secondary_comparison_summary.json`

Copy of [`output/models/strict_corporate_bridge_secondary_comparison_summary.json`](#outputmodelsstrict_corporate_bridge_secondary_comparison_summaryjson).

### `site/data/strict_component_framework_summary.json`

Copy of [`output/models/strict_component_framework_summary.json`](#outputmodelsstrict_component_framework_summaryjson).

### `site/data/strict_release_framing_summary.json`

Copy of [`output/models/strict_release_framing_summary.json`](#outputmodelsstrict_release_framing_summaryjson).

### `site/data/strict_direct_core_component_summary.json`

Copy of [`output/models/strict_direct_core_component_summary.json`](#outputmodelsstrict_direct_core_component_summaryjson).

### `site/data/strict_direct_core_horizon_stability_summary.json`

Copy of [`output/models/strict_direct_core_horizon_stability_summary.json`](#outputmodelsstrict_direct_core_horizon_stability_summaryjson).

### `site/data/strict_additional_creator_candidate_summary.json`

Copy of [`output/models/strict_additional_creator_candidate_summary.json`](#outputmodelsstrict_additional_creator_candidate_summaryjson).

### `site/data/strict_di_loans_nec_measurement_audit_summary.json`

Copy of [`output/models/strict_di_loans_nec_measurement_audit_summary.json`](#outputmodelsstrict_di_loans_nec_measurement_audit_summaryjson).

### `site/data/strict_results_closeout_summary.json`

Copy of [`output/models/strict_results_closeout_summary.json`](#outputmodelsstrict_results_closeout_summaryjson).

### `site/data/treasury_cash_regime_audit_summary.json`

Copy of [`output/models/treasury_cash_regime_audit_summary.json`](#outputmodelstreasury_cash_regime_audit_summaryjson).

### `site/data/historical_cash_term_reestimation_summary.json`

Copy of [`output/models/historical_cash_term_reestimation_summary.json`](#outputmodelshistorical_cash_term_reestimation_summaryjson).

### `site/data/toc_row_incidence_audit_summary.json`

Copy of [`output/models/toc_row_incidence_audit_summary.json`](#outputmodelstoc_row_incidence_audit_summaryjson).

### `site/data/toc_row_liability_incidence_raw_summary.json`

Copy of [`output/models/toc_row_liability_incidence_raw_summary.json`](#outputmodelstoc_row_liability_incidence_raw_summaryjson).

### `site/data/toc_validated_share_candidate_summary.json`

Copy of [`output/models/toc_validated_share_candidate_summary.json`](#outputmodelstoc_validated_share_candidate_summaryjson).

### `site/data/tdc_treatment_audit_summary.json`

Copy of [`output/models/tdc_treatment_audit_summary.json`](#outputmodelstdc_treatment_audit_summaryjson).

### `site/data/identity_measurement_ladder.csv`

Copy of [`output/models/identity_measurement_ladder.csv`](#outputmodelsidentity_measurement_laddercsv).

### `site/data/lp_irf_regimes.csv`

Copy of [`output/models/lp_irf_regimes.csv`](#outputmodelslp_irf_regimescsv).

### `site/data/regime_diagnostics_summary.json`

Copy of [`output/models/regime_diagnostics_summary.json`](#outputmodelsregime_diagnostics_summaryjson).

### `site/data/tdc_sensitivity_ladder.csv`

Copy of [`output/models/tdc_sensitivity_ladder.csv`](#outputmodelstdc_sensitivity_laddercsv).

### `site/data/control_set_sensitivity.csv`

Copy of [`output/models/control_set_sensitivity.csv`](#outputmodelscontrol_set_sensitivitycsv).

### `site/data/shock_sample_sensitivity.csv`

Copy of [`output/models/shock_sample_sensitivity.csv`](#outputmodelsshock_sample_sensitivitycsv).

### `site/data/period_sensitivity.csv`

Copy of [`output/models/period_sensitivity.csv`](#outputmodelsperiod_sensitivitycsv).

### `site/data/period_sensitivity_summary.json`

Copy of [`output/models/period_sensitivity_summary.json`](#outputmodelsperiod_sensitivity_summaryjson).

### `site/data/total_minus_other_contrast.csv`

Copy of [`output/models/total_minus_other_contrast.csv`](#outputmodelstotal_minus_other_contrastcsv).

### `site/data/structural_proxy_evidence.csv`

Copy of [`output/models/structural_proxy_evidence.csv`](#outputmodelsstructural_proxy_evidencecsv).

### `site/data/structural_proxy_evidence_summary.json`

Copy of [`output/models/structural_proxy_evidence_summary.json`](#outputmodelsstructural_proxy_evidence_summaryjson).

### `site/data/proxy_coverage_summary.json`

Copy of [`output/models/proxy_coverage_summary.json`](#outputmodelsproxy_coverage_summaryjson).

### `site/data/proxy_unit_audit.json`

Copy of [`output/models/proxy_unit_audit.json`](#outputmodelsproxy_unit_auditjson).

### `site/data/shock_diagnostics_summary.json`

Copy of [`output/models/shock_diagnostics_summary.json`](#outputmodelsshock_diagnostics_summaryjson).

### `site/data/headline_treatment_fingerprint.json`

Copy of [`output/models/headline_treatment_fingerprint.json`](#outputmodelsheadline_treatment_fingerprintjson).

### `site/data/provenance_validation_summary.json`

Copy of [`output/models/provenance_validation_summary.json`](#outputmodelsprovenance_validation_summaryjson).

### `site/data/direct_identification_summary.json`

Copy of [`output/models/direct_identification_summary.json`](#outputmodelsdirect_identification_summaryjson).

### `site/data/result_readiness_summary.json`

Copy of [`output/models/result_readiness_summary.json`](#outputmodelsresult_readiness_summaryjson).

### `site/data/pass_through_summary.json`

Copy of [`output/models/pass_through_summary.json`](#outputmodelspass_through_summaryjson).

### `site/data/tdcest_ladder_integration_summary.json`

Copy of [`output/models/tdcest_ladder_integration_summary.json`](#outputmodelstdcest_ladder_integration_summaryjson).

### `site/data/tdcest_broad_object_comparison_summary.json`

Copy of [`output/models/tdcest_broad_object_comparison_summary.json`](#outputmodelstdcest_broad_object_comparison_summaryjson).

### `site/data/tdcest_broad_treatment_sensitivity_summary.json`

Copy of [`output/models/tdcest_broad_treatment_sensitivity_summary.json`](#outputmodelstdcest_broad_treatment_sensitivity_summaryjson).

### `site/data/counterpart_channel_scorecard.json`

Copy of [`output/models/counterpart_channel_scorecard.json`](#outputmodelscounterpart_channel_scorecardjson).

### `site/data/deposit_type_side_read.csv`

Copy of [`output/models/deposit_type_side_read.csv`](#outputmodelsdeposit_type_side_readcsv).

### `site/data/sample_construction_summary.json`

Copy of [`output/models/sample_construction_summary.json`](#outputmodelssample_construction_summaryjson).
