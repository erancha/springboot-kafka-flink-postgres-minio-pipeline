export function ResultPanel({ result }: { result: string }) {
  return (
    <div className='section'>
      <div className='section__label'>Result</div>
      <pre className='result'>{result}</pre>
    </div>
  );
}
