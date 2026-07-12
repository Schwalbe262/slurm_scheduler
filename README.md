# Slurm Scheduler

Slurm job을 매번 새로 제출하지 않고, 미리 띄워 둔 warm allocation에 작업을 `srun --jobid`로 붙여 실행하는 웹 기반 스케줄러입니다. CPU FEA/RL 배치, 기존 원격 디렉터리 실행, Git 기반 작업, GPU/LLM 작업을 하나의 Web UI와 HTTP API로 다룹니다.

이 README는 사람이 먼저 전체 기능을 이해하도록 정리한 문서입니다. 상세 API, 설정, 장애 대응은 `docs/` 아래 문서에 분리되어 있습니다.

## 왜 만들었나

일반 Slurm 사용 방식에서는 작은 작업도 Slurm job 하나를 차지합니다. 계정별 job 개수 제한이 있는 환경에서는 수백 개의 짧은 FEA/RL 작업, Git 기반 실험, GPU 태스크를 효율적으로 넣기 어렵습니다.

이 프로젝트는 다음 방식으로 그 문제를 줄입니다.

- Slurm allocation job을 warm pool로 유지합니다.
- 실제 사용자 작업은 allocation 내부에 `srun --jobid` step으로 붙입니다.
- CPU, GPU, mixed capacity를 따로 추적하되 필요한 경우 안전하게 재사용합니다.
- Web UI와 JSON API에서 계정, allocation, task, GPU capacity, 로그 경로를 확인합니다.

## 현재 구현된 주요 기능

### 1. Warm Allocation Pool

- CPU warm pool과 GPU warm pool을 유지합니다.
- allocation 상태를 `pending`, `warm`, `active`, `draining`, `closing`, `closed`, `failed`로 관리합니다.
- 오래된 allocation은 drain 후 종료하고, idle allocation은 최소 개수를 지키며 scale-in합니다.
- queued task가 현재 capacity에 맞지 않으면 새 allocation을 demand 기반으로 엽니다.

### 2. Attached Task 실행

기존 원격 디렉터리에 있는 프로젝트를 그대로 실행할 수 있습니다.

```bash
curl -sS -X POST "$SCHEDULER_URL/tasks" \
  -F name=fea-case-001 \
  -F remote_cwd=/remote/project/path \
  -F command='python run_fea.py --case case001' \
  -F cpus=4 \
  -F memory_mb=8192 \
  -F scheduling_profile=standard \
  -F gpus=0
```

스케줄러는 적합한 allocation을 찾고, 해당 allocation job 안에서 `srun --jobid=<allocation>`으로 task script를 실행합니다.

### 3. FEA Bursty Scheduling Profile

FEA 작업은 CPU와 메모리를 항상 고정량으로 점유하지 않는 경우가 많습니다. 이를 위해 attached task에 `scheduling_profile=fea_bursty`가 추가되어 있습니다.

동작 방식:

- `cpus`는 peak CPU 요청으로 유지하지만 scheduler slot 계산에서는 hard reservation으로 쓰지 않습니다.
- `memory_mb`는 FEA task의 예상 peak/safety 메타데이터로 보존하며, 각 step의 독점 메모리 예약으로 쓰지 않습니다.
- 신규 FEA attach는 `pestat`의 node free memory와 CPU load를 보고 결정합니다.
- free memory가 soft threshold 미만이면 새 FEA attach를 막습니다.
- free memory가 hard threshold 미만이면 해당 allocation에서 가장 늦게 붙은 running FEA task를 실패 처리하고 cancel합니다.
- allocation이 소유한 1x CPU baseline은 설정된 node당 attach cap까지 먼저 채우고, 그 이후 overcommit worker만 node당 tick마다 최대 2개씩 추가하며 2x를 넘지 않습니다.
- CPU·GPU allocation은 `fea_bursty`와 `standard` 중 먼저 활성화된 profile 전용으로 사용하며, `same_node_as`도 같은 profile 사이에서만 공동배치됩니다. CPU-only FEA는 실제 GPU pool을 점유하지 않지만 GPU가 없는 CPU allocation이 GPU partition에 배치된 경우는 사용할 수 있습니다.
- FEA task는 `--overlap --cpu-bind=none`으로 allocation의 CPU·메모리 풀을 공유합니다. `standard`, same-node client, vLLM 경로의 per-step `--mem` 동작은 그대로 유지됩니다.

기본 정책:

```yaml
fea_bursty:
  soft_memory_free_percent: 60
  hard_memory_free_percent: 40
  load_target: 0.75
  max_attach_per_loop: 8
  max_attach_per_node_per_loop: 12
  shared_memory_estimate_fraction: 0.25
  shared_memory_min_estimate_mb: 8192
  node_name_policy: preferred
  overload_scale_out_load_factor: 2.0
  overload_scale_out_seconds: 300
```

예시:

```bash
curl -sS -X POST "$SCHEDULER_URL/tasks" \
  -F name=fea-bursty-001 \
  -F remote_cwd=/remote/fea/project \
  -F command='python run_fea.py --case 001' \
  -F cpus=4 \
  -F memory_mb=32768 \
  -F scheduling_profile=fea_bursty \
  -F gpus=0
```

### 4. Git 기반 작업

Git repo를 clone/update한 뒤 Python entrypoint를 실행하는 task를 만들 수 있습니다.

```bash
curl -sS -X POST "$SCHEDULER_URL/tasks/git" \
  -F job_name=git-case-001 \
  -F repo_url=git@github.com:org/private-repo.git \
  -F git_ref=main \
  -F entrypoint=scripts/run.py \
  -F arguments='--case case001' \
  -F cpus=4 \
  -F memory=8G \
  -F scheduling_profile=standard \
  -F gpus=0
```

Private repo는 중앙 Git credential 설정을 통해 처리할 수 있습니다. 각 Slurm 계정 홈에 GitHub key를 복사하지 않고, scheduler가 task 임시 디렉터리에 deploy key를 주입하고 `GIT_SSH_COMMAND`를 설정합니다.

### 5. GPU Scheduling

- GPU warm pool을 유지할 수 있습니다.
- 기본 우선순위는 A6000 ADA, A6000 순서입니다.
- task는 `gpus`, `gpu_model`, `partition`, `node_name`을 요청할 수 있습니다.
- `gpu_model=a6000ada,a6000`처럼 ordered fallback 후보를 받을 수 있습니다.
- GPU capacity는 물리 총량뿐 아니라 cluster used/free, scheduler owned/free를 분리해서 보여줍니다.

GPU task 예시:

```bash
curl -sS -X POST "$SCHEDULER_URL/tasks" \
  -F name=llm-a6000ada \
  -F remote_cwd=/remote/llm/project \
  -F command='python run_inference.py --model /models/model-name' \
  -F cpus=8 \
  -F memory_mb=32768 \
  -F gpus=1 \
  -F gpu_model=a6000ada \
  -F partition=auto
```

### 6. Mixed CPU/GPU Capacity

CPU-only task는 CPU allocation을 우선 사용합니다. 필요한 경우 GPU allocation 안의 남는 CPU도 빌릴 수 있습니다.

다만 GPU allocation의 free GPU를 위해 CPU reserve를 남깁니다.

```text
borrowable_cpu = free_cpus - (free_gpus * gpu_prewarm.cpu_reserve_per_free_gpu)
```

즉 GPU를 잡아두고 있는 allocation이라고 해서 CPU-only task가 모든 CPU를 가져가지는 않습니다.

### 7. Dynamic Packed FEA/RL Batch

많은 simulation case를 한 번에 돌리는 기존 packed workflow도 유지됩니다.

`dynamic_packed_srun`은 저장된 `pestat` 데이터를 기반으로 node별 CPU load, free memory, CPU/thread 요구량을 보고 packed Slurm job 계획을 만듭니다.

```bash
curl -sS -X POST "$SCHEDULER_URL/jobs" \
  -F job_mode=dynamic_packed_srun \
  -F remote_path=/remote/project/path \
  -F entrypoint=scripts/run_fea.py \
  -F arguments='--campaign sweep-001' \
  -F total_simulations=20 \
  -F cpus_per_simulation=4 \
  -F mem_per_simulation_gb=8 \
  -F max_workers_per_job=20 \
  -F max_new_jobs=10 \
  -F time_limit=48:00:00 \
  -F partition=auto
```

일반 작업은 attached task가 기본 경로이고, packed job은 다수 simulation을 하나의 batch allocation 안에서 orchestrate해야 할 때 사용합니다.

### 8. Capability / Env Profile Routing

계정마다 설치된 conda 환경이나 소프트웨어가 다를 수 있습니다. 그래서 task는 다음 필드로 실행 가능한 계정을 제한할 수 있습니다.

- `required_capability`: 예를 들어 `conda:pyaedt2026v1`
- `env_profile`: 실제 shell setup을 prepend할 profile 이름

계정 설정 예시:

```yaml
capabilities: ["conda:pyaedt2026v1"]
env_profiles:
  pyaedt2026v1: |
    source ~/miniconda3/etc/profile.d/conda.sh
    conda activate pyaedt2026v1
```

Web UI의 Capabilities 섹션과 `/api/capabilities`에서 어떤 account가 어떤 capability를 지원하는지 확인할 수 있습니다.

### 9. Conda Env Sync

reference account의 conda 환경을 다른 account로 복제하는 API가 있습니다.

- `conda-pack` 기반으로 환경을 묶습니다.
- target account에 같은 이름의 환경이 있으면 timestamp backup으로 옮깁니다.
- 설치가 끝나면 `conda:<env-name>` capability와 matching `env_profile` overlay를 DB에 기록합니다.

```bash
curl -sS -X POST "$SCHEDULER_URL/api/conda-env-sync" \
  -H 'Content-Type: application/json' \
  --data '{
    "reference_account": "account_a",
    "source_env_name": "pyaedt2026v1",
    "target_accounts": ["account_b", "account_c"]
  }'
```

### 10. Web UI와 운영 API

Web UI에서 볼 수 있는 것:

- 계정별 running/pending/limit/storage
- capability와 account 매핑
- active/closed allocation pool
- active/finished attached task
- active/finished direct job
- GPU capacity
- token usage
- conda env sync 상태
- task detail, stdout/stderr path, recreate payload

주요 API:

```bash
curl -sS "$SCHEDULER_URL/api/health"
curl -sS "$SCHEDULER_URL/api/accounts/status"
curl -sS "$SCHEDULER_URL/api/allocations"
curl -sS "$SCHEDULER_URL/api/gpu-capacity"
curl -sS "$SCHEDULER_URL/api/task-capacity?cpus=4&memory_mb=32768&scheduling_profile=fea_bursty"
curl -sS "$SCHEDULER_URL/api/tasks"
curl -sS "$SCHEDULER_URL/api/tasks/<task_id>/stdout"
curl -sS "$SCHEDULER_URL/api/tasks/<task_id>/stderr?tail_lines=100"
curl -sS "$SCHEDULER_URL/api/tasks/<task_id>/remote-file?base=remote_cwd&path=results/out.json"
```

`/api/task-capacity`는 `scheduling_profile=fea_bursty`일 때 `memory_pressure_state`를 반환합니다.

```text
ok | soft_blocked | hard_pressure
```

### 11. Remote Output과 Cleanup

task detail과 API에서 stdout/stderr, remote file, remote glob을 조회할 수 있습니다.

오래된 scheduler-created remote directory는 자동 cleanup 대상입니다.

- `task-*`
- `job-*`
- `allocation-*`

기본 TTL은 finished task/job 7일, closed allocation 1일입니다.

### 12. Token Usage 기록

Codex나 LLM 작업량을 프로젝트별로 기록하고 Web UI에서 그래프와 테이블로 볼 수 있습니다.

```bash
curl -sS -X POST "$SCHEDULER_URL/token-usage" \
  -F provider=codex \
  -F project=slurm_scheduler \
  -F input_tokens=1000 \
  -F output_tokens=500 \
  -F reset_cycle=2026-W24 \
  -F note='example run'
```

## 빠른 시작

```bash
git clone https://github.com/Schwalbe262/slurm_scheduler.git
cd slurm_scheduler

sudo apt update
sudo apt install -y python3.12-venv python3-pip

cp config/app.example.yaml config/app.yaml
cp config/accounts.example.yaml config/accounts.yaml
```

`config/accounts.yaml`에 실제 Slurm login account, SSH key path, remote workspace, job limit, capability/profile을 입력합니다.

설치와 smoke check:

```bash
bash scripts/setup_and_smoke.sh
. .venv/bin/activate
python3 -m slurm_scheduler
```

기본 접속:

```text
http://127.0.0.1:8000/
```

다른 장비에서 접근하려면 trusted LAN/VPN/Tailscale 뒤에 두고 `bind_host`, firewall, reverse proxy 설정을 확인하세요. 이 Web UI에는 자체 로그인 기능이 없습니다.

## 설정 파일

runtime 설정은 Git에 올리지 않습니다.

```bash
cp config/app.example.yaml config/app.yaml
cp config/accounts.example.yaml config/accounts.yaml
```

중요 설정:

- `cluster_refresh_interval_seconds`: inventory와 `pestat` refresh 주기
- `min_warm_allocations`: CPU warm allocation 최소 개수
- `allocation_cpus`: CPU warm pool 목표/상한. CPU-only 노드는 작은 fragment를 피하고 가능한 큰 pool로 요청하며, CPU-only 노드가 꽉 찬 경우 GPU 노드의 `gpu_cpu_reserve` 제외 빈 CPU에 맞춰 요청합니다.
- `cpu_pool_allow_gpu_partitions`: CPU pool이 GPU partition의 CPU를 사용할 수 있는지
- `cpu_partition_allocation_limits`: partition 내 물리 노드별 CPU pool live 상한. 기본적으로 `cpu2`는 한 노드에 CPU pool을 최대 2개까지만 둡니다.
- `warm_pool_preferred_accounts`: CPU warm pool 선호 account
- `gpu_prewarm`: GPU warm pool 정책
- `fea_bursty`: bursty FEA pressure threshold
- `cleanup`: remote artifact cleanup TTL
- `git_credentials`: private Git repo credential 주입

상세 내용은 [docs/CONFIG.md](docs/CONFIG.md)를 보세요.

## 문서 지도

- [docs/API.md](docs/API.md): HTTP endpoint, form field, JSON response reference
- [docs/EXAMPLES.md](docs/EXAMPLES.md): copy-paste 운영 예시
- [docs/CONFIG.md](docs/CONFIG.md): app/account 설정
- [docs/scheduling-principles.md](docs/scheduling-principles.md): CPU/GPU/mixed scheduling 정책
- [docs/gpu-scheduling.md](docs/gpu-scheduling.md): GPU prewarm, model priority, capacity 해석
- [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md): pending, SSH, inventory, task 문제 대응
- [docs/remote-access.md](docs/remote-access.md): LAN/VPN/Tailscale 접근
- [docs/llm-operator-guide.md](docs/llm-operator-guide.md): LLM agent가 API로 운영할 때의 짧은 가이드
- [docs/ROADMAP.md](docs/ROADMAP.md): 다음 개선 후보

## 테스트

로컬 단위 테스트:

```bash
python3 -m unittest tests.test_core
```

추가 정적 확인:

```bash
python3 -m compileall slurm_scheduler tests scripts
bash -n scripts/*.sh examples/*.sh
git diff --check
```

실제 Slurm 환경 점검:

```bash
python3 scripts/check_ssh.py --account account_a
python3 scripts/refresh_inventory.py --account account_a
python3 scripts/refresh_pestat.py --account account_a
python3 scripts/live_sleep_test.py --account account_a --count 1 --partition cpu_partition
```

live check는 실제 Slurm job을 제출합니다.

## 보안과 운영 주의

- 실제 host, account, key path, local `config/*.yaml`, credential은 Git에 올리지 않습니다.
- Web UI에는 자체 인증이 없습니다. private network 또는 authenticated reverse proxy 뒤에서 운영하세요.
- scheduler는 각 account의 `remote_workspace` 아래에서 만든 `task-*`, `job-*`, `allocation-*` artifact만 cleanup합니다.
- `memory_mb`는 RAM을 미리 물리적으로 할당한다는 뜻이 아니라 scheduler reservation 및 Slurm step memory limit입니다.
- `fea_bursty`는 bursty workload용입니다. 항상 고정 CPU/memory를 써야 하는 작업은 `standard`를 사용하세요.
