import argparse,json,os
from .reference_engine import Engine
from . import core

def main():
    p=argparse.ArgumentParser(); p.add_argument('--root',default='reference-runs'); sub=p.add_subparsers(dest='command',required=True)
    a=sub.add_parser('start'); a.add_argument('scenario_file')
    for cmd in ('status','advance','audit','metrics','stop'):
        a=sub.add_parser(cmd); a.add_argument('run_id')
    a=sub.add_parser('approve'); a.add_argument('run_id'); a.add_argument('hash'); a.add_argument('--reject',action='store_true')
    a=sub.add_parser('clarify'); a.add_argument('run_id'); a.add_argument('answer')
    a=sub.add_parser('revise'); a.add_argument('run_id'); a.add_argument('stage'); a.add_argument('--requirement')
    args=p.parse_args(); e=Engine(args.root); core.ROOT=e.root
    if args.command=='start': result=e.start(**json.load(open(args.scenario_file,encoding='utf8')))
    elif args.command=='approve': result=e.respond(args.run_id,{'approve':not args.reject,'hash':args.hash})
    elif args.command=='clarify': result=e.respond(args.run_id,args.answer)
    elif args.command=='revise': result=e.revise(args.run_id,args.stage,requirement=args.requirement)
    else: result=getattr(e,{'status':'load','advance':'advance','audit':'events','metrics':'metrics','stop':'stop'}[args.command])(args.run_id)
    print(json.dumps(result,indent=2,ensure_ascii=False))
if __name__=='__main__': main()
