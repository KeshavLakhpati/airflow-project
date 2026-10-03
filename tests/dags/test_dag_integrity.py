from airflow.models import DagBag


def test_dag_loads_cleanly():
    bag = DagBag(dag_folder="dags", include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    dag = bag.get_dag("ecommerce_pipeline")
    assert dag is not None
    assert {"init_tables", "seed_dimensions", "generate_orders",
            "build_staging", "build_marts", "quality_checks"} <= set(dag.task_ids)
