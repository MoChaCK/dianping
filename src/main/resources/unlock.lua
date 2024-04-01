---
--- Generated by EmmyLua(https://github.com/EmmyLua)
--- Created by 15925.
--- DateTime: 2024/2/7 10:56
---
--- 释放锁的lua脚本，确保判断锁和释放锁的原子性

-- 比较线程标识与锁中的标识是否一致
if (redis.call('get', KEYS[1]) == ARGV(1)) then
    -- 释放锁
    return redis.call('del', KEYS[1])
end
return 0