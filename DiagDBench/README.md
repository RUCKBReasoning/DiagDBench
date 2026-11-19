# DiagDBench Sample Archives

The training and evaluation samples accompanying
[DiagDBench: Evaluating Agentic LLMs on Interactive Database Diagnosis and Anomaly Remediation](https://arxiv.org/abs/2611.07339)
are available in the
[DiagDBench dataset](https://huggingface.co/datasets/zhaoxinxin798/DiagDBench)
on Hugging Face 🤗.

This directory contains only this README in the GitHub checkout; the archives
are downloaded separately.

| Archive on Hugging Face | Contents |
|---|---|
| `DiagDBench/Train.zip` | 6,940 training samples |
| `DiagDBench/Eval.zip` | 454 evaluation samples |

## Download

Run from the repository root:

```bash
python -m pip install -U huggingface_hub
hf download zhaoxinxin798/DiagDBench DiagDBench/Train.zip DiagDBench/Eval.zip \
  --repo-type dataset --local-dir .
```

The command saves `Train.zip` and `Eval.zip` in this directory without replacing
this README.

`Train.zip` contains `Train/MySQL/` and `Train/PostgreSQL/` sample directories.
`Eval.zip` contains an `Eval/` directory and its sample index,
`Eval/key_information.json`. For isolated diagnosis and evaluation, extract the
samples outside the repository and set the sample and index paths in your
configuration accordingly.
