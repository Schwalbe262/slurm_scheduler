# env_profile 추가 요청: MFT_1MW_2026v1

MFT 1MW 변압기 설계 캠페인(계정 6개, FEA 대량 병렬)용 환경 프로파일.
태스크마다 반복하던 모듈 로드 / 라이선스 설정 / 코드 스테이징을 중앙화한다.

## accounts.yaml 각 계정의 env_profiles에 추가

```yaml
MFT_1MW_2026v1: |
  source ~/miniconda3/etc/profile.d/conda.sh
  conda activate pyaedt2026v1
  source /etc/profile.d/lmod.sh 2>/dev/null || true
  module load ansys-electronics/v252 2>/dev/null || export ANSYSEM_ROOT252=/opt/ohpc/pub/Electronics/v252/Linux64
  export FLEXLM_TIMEOUT=3000000
  [ -d ~/slurm_scheduler/pyaedt_library/src ] || git clone -q --depth 1 https://github.com/Schwalbe262/pyaedt_library.git ~/slurm_scheduler/pyaedt_library
  [ -d ~/slurm_scheduler/MFT_1MW_2026v1 ] || git clone -q --depth 1 https://github.com/Schwalbe262/MFT_1MW_2026.git ~/slurm_scheduler/MFT_1MW_2026v1
  (cd ~/slurm_scheduler/MFT_1MW_2026v1 && git pull -q) || true
```

## 함께 필요한 것

- 반영을 위한 서비스 재시작 1회 (동시에 이 저장소에 커밋된 /api/tasks의
  limit / name_prefix 파라미터도 활성화됨 - CHANGELOG_claude.md 참조)
- dhj02 계정 .local(29G)/.cache(22G) 정리 (별도 전달된 건)

## 사용 방식 (참고)

- 태스크는 `env_profile=MFT_1MW_2026v1`로 제출되고, 명령은
  `cd ~/slurm_scheduler/MFT_1MW_2026v1 && python run_simulation_260706.py ...`로 단순화
- 프로젝트 파일명은 uuid 기반이라 같은 폴더에서 동시 실행 안전 (클라이언트 코드 측 처리)
- 임시파일 청소는 계정당 `MFT_1MW_2026v1/simulation` 폴더 하나만 주기 스윕
