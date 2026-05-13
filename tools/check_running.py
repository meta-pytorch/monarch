# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.
import asyncio

from libfb.py.asyncio.sandcastle import ApiException, AsyncSandcastleClient


async def main() -> None:
    async with AsyncSandcastleClient() as client:
        instances = await client.get_info(job_id=None, diff_id=381473186)
    slow = sorted(
        [
            i
            for i in instances
            if (i.get("elapsedTime") or 0) > 30 * 60
            and "test-mode" in (i.get("alias") or "")
            and "shadow" not in (i.get("alias") or "")
        ],
        key=lambda i: -(i.get("elapsedTime") or 0),
    )[:3]
    async with AsyncSandcastleClient() as c2:
        for inst in slow:
            e = inst.get("elapsedTime", 0)
            print(f"\n{e // 60}m  [{inst.get('statusString')}]  {inst.get('alias')}")
            try:
                spec = await c2.get_spec(inst["id"])
                for t in sorted(
                    spec.get("args", {}).get("targets", []),
                    key=lambda t: t.get("depth", 99),
                ):
                    print(f"  depth={t.get('depth')}  {t.get('target')}")
            except Exception as ex:
                print(f"  {ex}")


asyncio.run(main())
